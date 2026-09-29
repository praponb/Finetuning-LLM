WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, elem
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(promo) AS promo_upper,
        COALESCE(promo, 'NONE') AS promo_filled
    FROM lines
    WHERE promo IS DISTINCT FROM 'SPRING10'
      AND created_at > DATE '2025-03-02'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        created_at,
        pos,
        price,
        promo,
        promo_filled,
        SUM(price) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS price_sum_2h
    FROM valid
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, HOUR(created_at) AS created_at_hour
    FROM metrics
    WHERE price BETWEEN 190 AND 200
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        price_sum_2h - created_at_hour AS price_sum_2h_minus_created_at_hour,
        COALESCE(promo, 'N/A') AS resolved_promo
    FROM cleaned
    WHERE created_at >= DATE '2025-03-01'
      AND promo IS NOT NULL
)
SELECT
    created_at,
    pos,
    promo,
    promo_filled,
    price_sum_2h,
    created_at_hour,
    price_sum_2h_minus_created_at_hour,
    resolved_promo
FROM filtered
ORDER BY created_at_hour DESC NULLS FIRST;
