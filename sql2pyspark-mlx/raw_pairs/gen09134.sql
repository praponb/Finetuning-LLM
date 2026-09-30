WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        UPPER(status) status_upper,
        DATE_SUB(shipped_date, 73) shipped_date_minus_73d
    FROM shipments
    WHERE shipped_date > DATE '2023-05-30'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        LEAD(cost, 1, 0) OVER (PARTITION BY carrier ORDER BY shipped_date, id) next_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY DATE_TRUNC('YEAR', shipped_date), status), 4) cost_share,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date_minus_73d, id) event_seq,
        CUME_DIST() OVER (PARTITION BY carrier, status ORDER BY cost DESC NULLS FIRST) cost_cume_dist
    FROM base
    WHERE cost < 82
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        cost_share,
        event_seq,
        cost_cume_dist,
        ROUND(cost_share / event_seq, 2) cost_share_per_event_seq,
        CEIL(event_seq) event_seq_whole,
        COALESCE(carrier, 'NONE') carrier_clean
    FROM enriched
    WHERE carrier IN ('UPS', 'DHL', 'Kerry')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT
    SELECT id
    FROM shipments
    WHERE shipped_date BETWEEN '2023-12-07' AND '2025-02-20'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        cost_share,
        event_seq,
        cost_share_per_event_seq,
        event_seq_whole,
        carrier_clean,
        LEAST(event_seq_whole, cost_share) least_event_seq_whole_cost_share
    FROM reconciled
    WHERE cost_share BETWEEN 0.35 AND 0.38
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq_whole >= 72 THEN 'HIGH' ELSE 'LOW' END AS event_seq_whole_tier,
    ROUND(AVG(cost_share_per_event_seq), 2) AS avg_cost_share_per_event_seq,
    ROUND(AVG(cost_share), 2) AS avg_cost_share,
    ROUND(AVG(least_event_seq_whole_cost_share), 2) AS avg_least_event_seq_whole_cost_share,
    APPROX_PERCENTILE(cost_share_per_event_seq, 0.95) AS cost_share_per_event_seq_p95,
    MAX(event_seq) AS max_event_seq
FROM cleaned
GROUP BY CASE WHEN event_seq_whole >= 72 THEN 'HIGH' ELSE 'LOW' END;
