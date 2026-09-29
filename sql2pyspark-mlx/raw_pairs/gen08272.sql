WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.shipped_date,
        a.delivered_date,
        p.channel,
        p.order_date
    FROM ranked a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(delivered_date, order_date) / 7 AS weeks_order_date_to_delivered_date,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM joined
    WHERE channel IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('week', order_date) AS order_date_period,
        INITCAP(TRIM(channel)) AS channel_title
    FROM filtered
    WHERE shipped_date > TO_DATE('2023-03-16')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(weeks_order_date_to_delivered_date) OVER w_ordered AS running_max_weeks_order_date_to_delivered_date,
        COUNT(weeks_order_date_to_delivered_date) OVER w_ordered AS running_count_weeks_order_date_to_delivered_date,
        MIN(weeks_order_date_to_delivered_date) OVER w_ordered AS running_min_weeks_order_date_to_delivered_date,
        AVG(weeks_order_date_to_delivered_date) OVER w_ordered AS running_avg_weeks_order_date_to_delivered_date,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY channel ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM valid
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    o.channel AS order_channel,
    e.channel AS shipment_channel,
    COUNT(*) AS rows_total,
    MAX(e.weeks_order_date_to_delivered_date) AS max_weeks_order_date_to_delivered_date,
    GROUPING_ID() AS level_id
FROM enriched e
INNER JOIN orders o ON e.order_id = o.id
GROUP BY GROUPING SETS ((o.channel, e.channel), (o.channel), ())
ORDER BY rows_total;
