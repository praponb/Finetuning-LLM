WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        shipping_fee,
        amount / NULLIF(discount, 0) AS amount_per_discount,
        CONCAT(channel, ', ', status) AS channel_status,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM orders
    WHERE customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY amount_per_discount, id) AS amount_per_discount_bucket,
        DENSE_RANK() OVER (ORDER BY shipping_fee NULLS FIRST) AS shipping_fee_dense_rank
    FROM filtered
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, customer_id ORDER BY order_date, id) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.age,
        c.email,
        c.country
    FROM latest src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM with_dims
    EXCEPT DISTINCT
    SELECT id, customer_id
    FROM orders
    WHERE status = 'SHIPPED'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.store_name,
        b.region,
        b.opened_date
    FROM verified src
    LEFT JOIN stores b
        ON src.store_id = b.id
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    amount,
    channel_status,
    channel_clean,
    age,
    email
FROM enriched
ORDER BY order_date, customer_id;
