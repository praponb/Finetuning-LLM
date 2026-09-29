WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        item.product_id
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) li
),
cleaned AS (
    SELECT *, CEIL(product_id) AS product_id_whole
    FROM flattened
    WHERE created_at < '2023-05-29'
),
with_windows AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id_whole,
        COUNT(product_id_whole) OVER (PARTITION BY DATE_TRUNC('YEAR', created_at)) AS created_at_period_count_product_id_whole,
        SUM(product_id) OVER w_grp AS product_id_sum_30m
    FROM cleaned
    WINDOW w_grp AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
cleaned_carts AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id_whole,
        created_at_period_count_product_id_whole,
        product_id_sum_30m,
        product_id_whole + product_id_sum_30m AS product_id_whole_plus_product_id_sum_30m
    FROM with_windows
    WHERE created_at_period_count_product_id_whole != 7
),
matched_keys AS (
    SELECT id
    FROM cleaned_carts
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
reconciled AS (
    SELECT s.*
    FROM cleaned_carts s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    CASE WHEN product_id_whole_plus_product_id_sum_30m >= 84 THEN 'HIGH' ELSE 'LOW' END AS product_id_whole_plus_product_id_sum_30m_tier,
    CASE WHEN product_id_sum_30m >= 48 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_sum_30m_tier,
    AVG(created_at_period_count_product_id_whole) AS avg_created_at_period_count_product_id_whole,
    PERCENTILE_APPROX(product_id_sum_30m, ARRAY(0.25, 0.5, 0.75)) AS product_id_sum_30m_quartiles
FROM reconciled
GROUP BY 1, 2
