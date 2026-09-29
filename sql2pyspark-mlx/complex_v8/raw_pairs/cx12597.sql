WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('HEALTH', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE start_date < DATE '2025-05-02'
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.product_group,
        p.holder_name
    FROM recent s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        product_group,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product_group), 0), 4) AS premium_share_of_product_group,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        MAX(premium) OVER w_product_group AS product_group_max_premium,
        premium - LAG(premium) OVER (PARTITION BY holder_id ORDER BY end_date ASC, id) AS premium_change,
        SUM(premium) OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_sum_1_prior,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, product_group ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        SUM(premium) OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_sum_3,
        MAX(premium) OVER (PARTITION BY holder_id ORDER BY end_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_max_7,
        MIN(premium) OVER (PARTITION BY product_group) AS product_group_min_premium,
        ROUND(PERCENT_RANK() OVER w_status_by_end_date, 3) AS end_date_percent_rank
    FROM enriched_holder
    WINDOW w_product_group AS (PARTITION BY product_group), w_status_by_end_date AS (PARTITION BY status ORDER BY end_date DESC, id)
)
SELECT
    id,
    product,
    start_date,
    end_date,
    product_group,
    premium_share_of_product_group,
    first_known_start_date,
    product_group_max_premium,
    premium_change,
    premium_sum_1_prior,
    premium_sum_3,
    premium_max_7,
    product_group_min_premium,
    end_date_percent_rank,
    LENGTH(TRIM(status)) AS status_len,
    DATE_TRUNC('QUARTER', first_known_start_date) AS first_known_start_date_quarter
FROM with_windows
ORDER BY first_known_start_date, end_date_percent_rank
