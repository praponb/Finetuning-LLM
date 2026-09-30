WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        amount,
        CASE WHEN status IN ('SHIPPED', 'CANCELLED') THEN 'Priority' ELSE 'Other' END AS status_group,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM orders
    WHERE order_date > TO_DATE('2023-08-28')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_date,
        channel,
        amount,
        status_group,
        AVG(amount) OVER (PARTITION BY channel, customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MIN(amount) OVER (PARTITION BY channel, customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        COUNT(amount) OVER (PARTITION BY channel, customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) AS row_seq
    FROM filtered
    WHERE amount BETWEEN 1370 AND 1440
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_avg_amount NULLS LAST, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
        )
)
-- Multi-level aggregation
SELECT
    channel,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(*) AS num_rows,
    ROUND(AVG(row_seq), 2) AS avg_row_seq_2,
    GROUPING_ID() AS gid
FROM kept
GROUP BY GROUPING SETS ((channel, DATE_TRUNC('WEEK', order_date)), (channel));
