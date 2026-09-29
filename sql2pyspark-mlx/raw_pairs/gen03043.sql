WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC NULLS FIRST) AS dedup_rank
    FROM shipments
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS row_seq,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_avg_5
    FROM latest
    WHERE order_id IN (7, 17, 36, 39)
),
latest_shipments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY cost_avg_5 DESC NULLS FIRST, id) AS dedup_rank_2
        FROM enriched
    ) ranked
    WHERE dedup_rank_2 = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_avg_5 DESC, id) AS rn
        FROM latest_shipments
    ) x
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    status,
    CASE WHEN row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered_rows,
    SUM(row_seq) FILTER (WHERE carrier = 'FedEx') AS fedex_total_row_seq
FROM ranked
GROUP BY CUBE (status, CASE WHEN row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
