WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        LOWER(device) AS device_lower
    FROM sessions
    WHERE country IS NOT NULL
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_seq,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM base
    WINDOW w_grp AS (PARTITION BY country, device ORDER BY started_at, id)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        started_at,
        country,
        seq_no,
        SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM sessionized
    INTERSECT
    SELECT customer_id, id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2025-03-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    e.country AS session_country,
    COUNT(*) AS cnt,
    SUM(e.seq_no) AS total_seq_no
FROM verified e
INNER JOIN customers dim ON e.customer_id = dim.id
GROUP BY dim.segment, e.country;
