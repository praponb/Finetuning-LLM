WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos + 1 AS position,
        line.product_id,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, line
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        product_id * price AS product_id_times_price,
        COALESCE(promo, 'UNKNOWN') AS resolved_promo
    FROM lines
    WHERE promo IS NOT NULL
      AND position IS NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo ORDER BY position DESC NULLS FIRST) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        position * row_num AS position_times_row_num,
        LEAST(position, product_id_times_price) AS least_position_product_id_times_price,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM deduped
    WHERE row_num <= 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        promo,
        product_id,
        COUNT(DISTINCT promo) AS distinct_promos,
        COUNT_IF(promo = 'WELCOME5') AS welcome5_events,
        MAX(created_at) AS last_created_at,
        PERCENTILE_APPROX(product_id_times_price, 0.5) AS product_id_times_price_p50,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned
    GROUP BY 1, 2
)
SELECT
    promo,
    product_id,
    welcome5_events,
    last_created_at,
    product_id_times_price_p50,
    distinct_ids
FROM totals;
