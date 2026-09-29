WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        line.product_id
    FROM (
        SELECT
            id,
            coupon_codes,
            EXPLODE_OUTER(items) AS line
        FROM carts
    ) x
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT product_id, EXPLODE_OUTER(coupon_codes) AS element
    FROM exploded
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(product_id AS DOUBLE) AS product_id_num,
        CAST(product_id AS DOUBLE) AS product_id_str
    FROM flattened
    WHERE element IS NOT NULL
      AND product_id >= 9
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        product_id,
        element,
        SUM(product_id_str) OVER (PARTITION BY element) AS element_sum_product_id_str,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY element), 4) AS product_id_share
    FROM valid
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(product_id_share) OVER (PARTITION BY element) AS element_max_product_id_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY element), 4) AS product_id_share_2,
        AVG(element_sum_product_id_str) OVER (PARTITION BY element) AS element_avg_element_sum_product_id_str
    FROM enriched
    WHERE element IN ('SPRING10', 'VIP', 'BUNDLE')
)
-- Window metrics over several different partitions
SELECT
    element_sum_product_id_str,
    product_id_share_2,
    element_avg_element_sum_product_id_str,
    RANK() OVER (ORDER BY element_max_product_id_share NULLS FIRST) AS element_max_product_id_share_rank,
    DENSE_RANK() OVER (PARTITION BY element ORDER BY element_avg_element_sum_product_id_str NULLS FIRST) AS element_avg_element_sum_product_id_str_dense_rank,
    MAX(element_max_product_id_share) OVER (PARTITION BY element) AS element_max_element_max_product_id_share
FROM scored;
