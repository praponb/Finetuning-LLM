WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), order_date) days_since_order_date,
        COALESCE(channel, 'N/A') channel_clean
    FROM orders
    WHERE order_date > DATE '2023-04-01'
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id, customer_id
    FROM base
    EXCEPT
    SELECT store_id, customer_id
    FROM orders
    WHERE amount BETWEEN 510 AND 1480
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.customer_id = k.customer_id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        LENGTH(channel) channel_length,
        days_since_order_date * discount days_since_order_date_times_discount
    FROM verified
    WHERE channel IS NOT DISTINCT FROM 'mobile'
      AND days_since_order_date > 30
),
cleaned_orders AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(status) clean_status,
        CASE WHEN status IN ('COMPLETED', 'CANCELLED') THEN 'Group A' ELSE 'Other' END status_group
    FROM cleaned
    WHERE days_since_order_date_times_discount IS NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        status,
        shipping_fee
    FROM cleaned_orders
) src
PIVOT (
    AVG(shipping_fee)
    FOR status IN ('RETURNED', 'COMPLETED', 'SHIPPED')
)
