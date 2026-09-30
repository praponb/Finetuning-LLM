WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE (YEAR(start_date) = 2025 OR end_date BETWEEN DATE '2025-12-05' AND DATE '2027-05-16')
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date
    FROM recent s
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_claim_date) AS first_claim_date_month,
        MIN(premium_bucket) AS min_premium_bucket,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        ROUND(SUM(premium), 2) AS total_premium,
        SUM(CASE WHEN product IN ('HOME', 'LIFE', 'TRAVEL') THEN premium_bucket ELSE 0 END) AS premium_bucket_product_in_home_life_travel,
        MAX(end_date) AS last_end_date
    FROM summarized_rows
    WHERE YEAR(end_date) = 2026
    GROUP BY DATE_TRUNC('MONTH', first_claim_date)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        first_claim_date_month,
        min_premium_bucket,
        distinct_holder_ids,
        median_premium,
        total_premium,
        premium_bucket_product_in_home_life_travel,
        last_end_date,
        MIN(total_premium) OVER (PARTITION BY distinct_holder_ids) AS distinct_holder_ids_min_total_premium,
        NTILE(4) OVER w_last_end_date_month_distinct AS median_premium_quartile
    FROM summary
    WINDOW w_last_end_date_month_distinct AS (PARTITION BY MONTH(last_end_date), distinct_holder_ids ORDER BY median_premium, first_claim_date_month)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(distinct_holder_ids, median_premium_quartile) AS greatest_distinct_holder_ids_median_premium_quar,
        ROUND(min_premium_bucket / NULLIF(median_premium_quartile, 0), 3) AS min_premium_bucket_per_median_premium_quartile
    FROM with_windows
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(last_end_date, 'yyyy-MM') AS last_end_date_label,
        DATE_FORMAT(last_end_date, 'yyyy-MM') AS last_end_date_label_2
    FROM derived
    WHERE HOUR(first_claim_date_month) BETWEEN 9 AND 11
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', last_end_date_label, last_end_date_label_2) AS last_end_date_label_last_end_date_label_2_label,
        DATEDIFF(DATE '2025-06-30', first_claim_date_month) AS days_since_first_claim_date_month,
        DATE_FORMAT(first_claim_date_month, 'yyyy-MM-dd') AS first_claim_date_month_label
    FROM annotated
)
-- Final projection
SELECT
    first_claim_date_month,
    distinct_holder_ids,
    median_premium,
    premium_bucket_product_in_home_life_travel,
    last_end_date,
    greatest_distinct_holder_ids_median_premium_quar,
    last_end_date_label,
    last_end_date_label_2,
    last_end_date_label_last_end_date_label_2_label,
    days_since_first_claim_date_month,
    SUBSTRING(TRIM(last_end_date_label), 1, 2) AS last_end_date_label_prefix,
    distinct_holder_ids - median_premium_quartile AS distinct_holder_ids_minus_median_premium_quartil
FROM tagged
ORDER BY last_end_date_label_2, days_since_first_claim_date_month
