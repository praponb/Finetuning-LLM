WITH joined AS (
    -- Attach orders attributes
    SELECT e.*, b.channel
    FROM shipments e
    INNER JOIN orders b
        ON e.order_id <=> b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(shipped_date) ORDER BY cost, id) AS row_seq,
        SUM(cost) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        SUM(cost) OVER (PARTITION BY carrier, channel) AS carrier_sum_cost
    FROM joined
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        cost,
        status,
        channel,
        row_seq,
        cumulative_cost,
        carrier_sum_cost,
        UPPER(channel) AS channel_upper,
        DATEDIFF(CURRENT_DATE(), shipped_date) / 7 AS weeks_since_shipped_date,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM metrics
    WHERE delivered_date > TO_DATE('2023-05-09')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(carrier_sum_cost) OVER (PARTITION BY status ORDER BY carrier_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_carrier_sum_cost,
        COUNT(carrier_sum_cost) OVER (PARTITION BY status ORDER BY carrier_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_carrier_sum_cost,
        AVG(carrier_sum_cost) OVER (PARTITION BY status ORDER BY carrier_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_carrier_sum_cost,
        MIN(carrier_sum_cost) OVER (PARTITION BY status ORDER BY carrier_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_carrier_sum_cost,
        AVG(cost) OVER (ORDER BY carrier_sum_cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior
    FROM cleaned
    WHERE order_id IN (4, 27, 36, 37)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id, status ORDER BY running_sum_carrier_sum_cost DESC) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_min_carrier_sum_cost) OVER (PARTITION BY order_id ORDER BY running_min_carrier_sum_cost, id) AS next_running_min_carrier_sum_cost,
        LAG(running_min_carrier_sum_cost) OVER (PARTITION BY order_id ORDER BY running_min_carrier_sum_cost, id) AS prev_running_min_carrier_sum_cost,
        LEAD(cost_avg_2_prior) OVER (PARTITION BY order_id ORDER BY running_min_carrier_sum_cost, id) AS next_cost_avg_2_prior,
        LAG(cost_avg_2_prior) OVER (PARTITION BY order_id ORDER BY running_min_carrier_sum_cost, id) AS prev_cost_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_min_carrier_sum_cost, id) AS seq_no,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY running_sum_carrier_sum_cost DESC, id) AS running_sum_carrier_sum_cost_bucket
    FROM deduped
)
-- Multi-level aggregation
SELECT
    order_id,
    channel,
    CASE WHEN running_sum_carrier_sum_cost >= 2040 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_carrier_sum_cost_tier,
    SUM(CASE WHEN channel = 'web' THEN carrier_sum_cost ELSE 0 END) AS web_carrier_sum_cost,
    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered_rows
FROM scored
GROUP BY GROUPING SETS ((order_id, channel, CASE WHEN running_sum_carrier_sum_cost >= 2040 THEN 'LARGE' ELSE 'SMALL' END), (order_id, channel), (order_id), ())
