WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        ROUND(amount, 1) AS amount_rounded,
        CONCAT_WS('-', channel, status) AS channel_status_key,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM orders
    WHERE channel IS DISTINCT FROM 'mobile'
      AND order_date >= DATE '2023-09-07'
),
cleaned_orders AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(amount_rounded * 100, 2) AS adjusted_amount_rounded,
        ADD_MONTHS(order_date, 1) AS order_date_plus_months
    FROM cleaned
    WHERE order_date > DATE '2023-09-08'
      AND channel IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.age,
        b.country,
        b.credit_limit
    FROM cleaned_orders src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.city = 'Singapore'
),
joined_orders AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.channel,
        src.amount,
        src.discount,
        src.shipping_fee,
        src.amount_rounded,
        src.channel_clean,
        src.adjusted_amount_rounded,
        src.order_date_plus_months,
        src.age,
        b.country AS customer_country,
        b.credit_limit AS customer_credit_limit
    FROM joined src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.channel,
        src.amount_rounded,
        src.channel_clean,
        src.order_date_plus_months,
        src.age,
        c.name,
        c.signup_date,
        c.age AS customer_age
    FROM joined_orders src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        channel,
        amount_rounded,
        channel_clean,
        order_date_plus_months,
        age,
        COUNT(amount_rounded) OVER (PARTITION BY channel ORDER BY customer_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount_rounded,
        AVG(amount_rounded) OVER (PARTITION BY channel ORDER BY customer_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount_rounded,
        MAX(amount_rounded) OVER (PARTITION BY channel ORDER BY customer_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount_rounded
    FROM enriched
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, b.age AS customer_age
    FROM scored s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
-- Grouped metrics
SELECT
    customer_id,
    SUM(last7_max_amount_rounded) FILTER (WHERE channel = 'partner') AS partner_total_last7_max_amount_rounded,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows
FROM with_dims
GROUP BY customer_id
HAVING SUM(last7_max_amount_rounded) FILTER (WHERE channel = 'partner') < 1000000
