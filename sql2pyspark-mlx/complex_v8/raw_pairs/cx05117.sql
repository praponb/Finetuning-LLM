WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN product IN ('LIFE', 'TRAVEL') THEN 'group_a' WHEN product = 'HEALTH' THEN 'group_b' ELSE 'other' END AS product_group,
        CONCAT_WS(': ', product, status) AS product_status_label,
        ABS(premium - 2319.8) AS premium_distance
    FROM policies
    WHERE YEAR(end_date) = 2026
      AND premium >= 2319.8
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(premium_distance, 2, 0) OVER w_start_date_year_by_start_dat AS prev2_premium_distance,
        MIN(premium_distance) OVER (PARTITION BY product_group, holder_id) AS product_group_holder_id_min_premium_distance
    FROM filtered
    WINDOW w_start_date_year_by_start_dat AS (PARTITION BY YEAR(start_date) ORDER BY start_date ASC, id)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE premium_distance < (SELECT MAX(premium_distance) FROM windowed)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    product_group,
    product_status_label,
    premium_distance,
    prev2_premium_distance,
    product_group_holder_id_min_premium_distance,
    CONCAT_WS('|', product, product_status_label) AS product_product_status_label_label
FROM with_activity
ORDER BY product_group, product DESC, id
LIMIT 20
