WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(status)) AS status_clean,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        LOWER(TRIM(status)) AS status_clean_2
    FROM policies
    WHERE holder_id <= 17
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(start_date) AS start_date_year,
        DATE_ADD(end_date, 30) AS end_date_plus_30d,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM scoped
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(start_date_year) OVER (PARTITION BY product ORDER BY start_date_year, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS start_date_year_sum_3_prior,
        ROUND(AVG(start_date_year) OVER (PARTITION BY product), 2) AS product_avg_start_date_year,
        LAG(end_date, 2) OVER (PARTITION BY product ORDER BY start_date_year, id) AS prev2_end_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product ORDER BY start_date_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        ROUND(start_date_year - AVG(start_date_year) OVER (PARTITION BY product), 2) AS start_date_year_vs_product_avg
    FROM tagged
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE premium < (SELECT MAX(premium) FROM windowed)
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.status <> 'REJECTED' THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_status_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at
    FROM screened s
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM summarized_rows
    WHERE start_date_year > (SELECT AVG(start_date_year) FROM summarized_rows)
)
-- Subtotals with CUBE
SELECT
    product,
    status,
    holder_id,
    ROUND(STDDEV(start_date_year), 2) AS stddev_start_date_year,
    ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null
FROM with_activity
GROUP BY CUBE(product, status, holder_id)
ORDER BY product NULLS LAST, status NULLS LAST, holder_id NULLS LAST
