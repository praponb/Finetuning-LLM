WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        ADD_MONTHS(delivered_date, 3) AS delivered_date_plus_months,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-08-09')
      AND order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY status, order_id ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST) AS cost_percent_rank,
        CUME_DIST() OVER (ORDER BY cost NULLS FIRST) AS cost_cume_dist
    FROM cleaned
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.delivered_date_plus_months,
        a.cost_percent_rank,
        d.channel,
        d.amount
    FROM enriched a
    INNER JOIN orders d
        ON a.order_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cost_percent_rank) OVER w_running AS prev_cost_percent_rank,
        LAG(amount) OVER w_running AS prev_amount,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM enriched_shipments
    WINDOW w_running AS (PARTITION BY channel ORDER BY cost_percent_rank, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        delivered_date_plus_months,
        channel,
        prev_cost_percent_rank,
        event_seq,
        RANK() OVER (PARTITION BY channel ORDER BY event_seq DESC NULLS LAST) AS event_seq_rank,
        NTILE(4) OVER (PARTITION BY channel ORDER BY prev_cost_percent_rank DESC, id) AS prev_cost_percent_rank_bucket
    FROM scored
)
-- Multi-level aggregation
SELECT
    channel,
    DATE_TRUNC('YEAR', delivered_date_plus_months) AS year_start,
    COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
    COUNT_IF(channel = 'store') AS store_events,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(event_seq_rank) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS event_seq_rank_per_web,
    DENSE_RANK() OVER (PARTITION BY channel ORDER BY COUNT_IF(channel = 'store') DESC) AS store_events_dense_rank
FROM with_windows
GROUP BY channel, DATE_TRUNC('YEAR', delivered_date_plus_months)
