WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date NULLS LAST, id) = 1
),
ranked_shipments AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY cost, id) AS latest_rank
        FROM ranked
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        LAG(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_cost,
        LEAD(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS row_seq
    FROM ranked_shipments
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(next_cost) OVER (ORDER BY next_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_cost,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY row_seq DESC, id) AS row_seq_bucket,
        LEAD(prev_cost, 1) OVER (PARTITION BY carrier ORDER BY row_seq, id) AS next_prev_cost
    FROM enriched
)
-- Join a dimension and aggregate
SELECT
    d.status,
    d.channel,
    COUNT(*) AS num_rows,
    SUM(e.row_seq) AS total_row_seq,
    GROUPING_ID() AS gid
FROM with_windows e
INNER JOIN orders d ON e.order_id = d.id
GROUP BY CUBE (d.status, d.channel)
ORDER BY channel DESC;
