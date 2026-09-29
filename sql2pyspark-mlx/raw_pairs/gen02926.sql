WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(status, ', ', channel) AS status_channel
    FROM orders
    WHERE YEAR(order_date) >= 2024
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, store_id ORDER BY order_date DESC, id) = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.customer_id,
        src.store_id,
        src.channel,
        src.status_channel,
        b.region,
        b.store_name,
        b.sqft
    FROM deduped src
    INNER JOIN stores b
        ON src.store_id = b.id
),
deduped_orders AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        status_channel,
        region,
        sqft
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY sqft DESC) AS latest_rank
        FROM joined
    ) x
    WHERE latest_rank = 1
),
joined_orders AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.status_channel,
        e.region,
        b.signup_date,
        b.city
    FROM deduped_orders e
    INNER JOIN customers b
        ON e.customer_id = b.id
    WHERE b.country <> 'UK'
)
-- Aggregate with conditional metrics
SELECT
    region,
    city,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2
FROM joined_orders
GROUP BY region, city
