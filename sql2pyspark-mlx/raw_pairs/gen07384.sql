WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END AS is_ups,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM shipments
    WHERE shipped_date >= DATE '2023-04-15'
      AND carrier IS DISTINCT FROM 'Kerry'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.amount,
        b.order_date,
        b.shipping_fee
    FROM base s
    INNER JOIN orders b
        ON s.order_id <=> b.id
    WHERE b.amount >= 900
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.shipped_date,
        src.delivered_date,
        src.status,
        src.carrier_clean,
        src.amount,
        src.shipping_fee,
        d.order_date AS order_order_date,
        d.channel
    FROM enriched src
    INNER JOIN orders d
        ON src.order_id <=> d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, order_id ORDER BY delivered_date NULLS FIRST, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        delivered_date,
        carrier_clean,
        channel,
        NTILE(3) OVER (PARTITION BY channel, status ORDER BY amount DESC, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY DATE_TRUNC('week', order_order_date), status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY channel ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        SUM(shipping_fee) OVER (PARTITION BY YEAR(shipped_date), channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
    FROM latest
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(running_shipping_fee) OVER w_part AS last7_min_running_shipping_fee,
        SUM(running_shipping_fee) OVER w_part AS last7_sum_running_shipping_fee,
        MAX(running_shipping_fee) OVER w_part AS last7_max_running_shipping_fee
    FROM with_windows
    WINDOW w_part AS (PARTITION BY channel ORDER BY running_shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(last7_min_running_shipping_fee AS INT) AS last7_min_running_shipping_fee_int,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM metrics
    WHERE last7_sum_running_shipping_fee < 600
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', delivered_date) AS week_start,
        SUM(CASE WHEN channel = 'mobile' THEN running_amount ELSE 0 END) AS mobile_running_amount,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        PERCENTILE_APPROX(last7_min_running_shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS last7_min_running_shipping_fee_quartiles,
        COUNT_IF(channel = 'web') AS web_events,
        AVG(running_shipping_fee) FILTER (WHERE channel = 'mobile') AS mobile_avg_running_shipping_fee
    FROM filtered
    GROUP BY week_start
)
SELECT
    week_start,
    mobile_running_amount,
    web_events
FROM stats
ORDER BY web_events NULLS LAST
