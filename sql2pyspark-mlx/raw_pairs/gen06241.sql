WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
base AS (
    SELECT
        id,
        host,
        latency_ms,
        attr_key,
        FLOOR(latency_ms) AS latency_ms_whole
    FROM lines
    WHERE log_ts > TIMESTAMP '2025-06-01 08:00:00'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host, attr_key ORDER BY latency_ms_whole, id, attr_key) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    SELECT
        id,
        latency_ms,
        attr_key,
        latency_ms_whole,
        dedup_rank,
        CONCAT(host, ' ', attr_key) AS host_attr_key,
        latency_ms_whole - latency_ms AS latency_ms_whole_minus_latency_ms,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM ranked
    WHERE attr_key IS NOT NULL
      AND latency_ms <> 2100
),
metrics AS (
    SELECT
        *,
        LEAD(latency_ms_whole) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS next_latency_ms_whole,
        LAG(latency_ms_whole) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS prev_latency_ms_whole,
        LEAD(latency_ms) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS next_latency_ms,
        LAG(latency_ms) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS prev_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms_whole_minus_latency_ms, id, attr_key) AS row_seq
    FROM cleaned
    WHERE latency_ms IS NOT NULL
),
filtered AS (
    SELECT
        id,
        latency_ms,
        attr_key,
        latency_ms_whole,
        host_attr_key,
        latency_ms_whole_minus_latency_ms,
        next_latency_ms_whole,
        next_latency_ms,
        row_seq,
        ROUND(row_seq / seq_no, 2) AS row_seq_per_seq_no
    FROM metrics
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'trace'
),
with_windows AS (
    SELECT
        *,
        COUNT(row_seq_per_seq_no) OVER (PARTITION BY attr_key) AS attr_key_count_row_seq_per_seq_no,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM filtered
    WINDOW w_hist AS (ORDER BY row_seq_per_seq_no DESC, id, attr_key)
)
SELECT *
FROM (
    SELECT
        id,
        attr_key,
        row_seq_per_seq_no,
        latency_ms_whole_minus_latency_ms
    FROM with_windows
) src
UNPIVOT (
    val FOR metric IN (row_seq_per_seq_no, latency_ms_whole_minus_latency_ms)
) u
