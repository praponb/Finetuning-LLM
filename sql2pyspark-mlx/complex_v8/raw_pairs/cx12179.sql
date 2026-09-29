WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        CONCAT_WS('|', priority, channel) AS priority_channel_label,
        COALESCE(csat, 0) AS csat_filled,
        QUARTER(closed_at) AS closed_at_quarter
    FROM tickets
    WHERE channel = 'chat'
),
scored AS (
    -- Label and score rows
    SELECT *, csat_filled - requester_id AS csat_filled_minus_requester_id
    FROM valid_tickets
),
latest_per_key AS (
    -- Keep the latest row per channel
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        priority_channel_label,
        csat_filled,
        closed_at_quarter,
        csat_filled_minus_requester_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY requester_id DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
rollup_base AS (
    -- Group-level totals
    SELECT
        closed_at_quarter,
        csat_filled,
        channel,
        PERCENTILE_APPROX(csat_filled, 0.5) AS median_csat_filled,
        SUM(requester_id) AS total_requester_id,
        COUNT(*) AS n_rows
    FROM latest_per_key
    GROUP BY closed_at_quarter, csat_filled, channel
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(n_rows, 0) AS n_rows_filled,
        ABS(closed_at_quarter - 3) AS closed_at_quarter_distance,
        COALESCE(n_rows, 0) AS n_rows_filled_2
    FROM rollup_base
)
-- Native SQL UNPIVOT (wide to long)
SELECT closed_at_quarter, csat_filled, metric_name, reading
FROM (
    SELECT closed_at_quarter, csat_filled, median_csat_filled, n_rows_filled_2, closed_at_quarter_distance, total_requester_id
    FROM tagged
) src
UNPIVOT (
    reading FOR metric_name IN (median_csat_filled, n_rows_filled_2, closed_at_quarter_distance, total_requester_id)
) unp
ORDER BY closed_at_quarter, csat_filled, metric_name
