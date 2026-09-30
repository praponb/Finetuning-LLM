WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        rating,
        LENGTH(supplier_name) AS supplier_name_length
    FROM suppliers
    WHERE city IS NOT NULL
      AND LOWER(supplier_name) LIKE '%ion%'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(rating) OVER (PARTITION BY country ORDER BY rating ASC, id ASC) AS next_rating,
        LEAD(supplier_name_length) OVER (PARTITION BY country ORDER BY rating ASC, id ASC) AS next_supplier_name_length,
        LAG(supplier_name_length) OVER (PARTITION BY country ORDER BY rating ASC, id ASC) AS prev_supplier_name_length,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating ASC, id ASC) AS event_seq,
        SUM(supplier_name_length) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_supplier_name_length
    FROM base
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        rating,
        next_rating,
        next_supplier_name_length,
        prev_supplier_name_length
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY cumulative_supplier_name_length ASC, id ASC) = 1
),
latest_suppliers AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY prev_supplier_name_length DESC) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.next_supplier_name_length,
        x.price,
        x.product_stock_qty
    FROM latest_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        price,
        UPPER(city) AS city_upper,
        COALESCE(city, 'NONE') AS city_filled
    FROM latest_match
    WHERE city IS NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        price,
        city_upper,
        MIN(price) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_price,
        SUM(price) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_price,
        AVG(price) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_price,
        COUNT(price) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_price
    FROM valid
    WHERE price >= 570
)
SELECT
    id,
    price,
    city_upper,
    last3_min_price,
    last3_avg_price,
    last3_count_price
FROM metrics;
