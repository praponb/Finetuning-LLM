WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        item.qty,
        item.price
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) li
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        coupon_codes,
        qty,
        price,
        price * qty AS price_times_qty,
        qty - price AS qty_minus_price
    FROM exploded
    WHERE customer_id IS NOT NULL
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        coupon_codes,
        qty,
        price,
        TO_DATE(created_at) AS created_at_date
    FROM valid
    WHERE price > 90
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid_carts
    EXCEPT
    SELECT id
    FROM carts
    WHERE created_at < '2024-12-13'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_carts s
    JOIN kept_keys k ON s.id = k.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END AS qty_tier,
        MIN(qty) AS min_qty,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(price), 2) AS avg_price
    FROM reconciled
    GROUP BY CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    qty_tier,
    min_qty,
    distinct_ids,
    avg_price
FROM daily;
