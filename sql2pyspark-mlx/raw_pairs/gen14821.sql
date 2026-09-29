WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC NULLS FIRST, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS row_seq
    FROM ranked
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, device
    FROM enriched
    INTERSECT
    SELECT customer_id, device
    FROM sessions
    WHERE country IN ('JP', 'UK')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.device = k.device
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.started_at
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.duration_sec <> 266
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.name,
        c.signup_date
    FROM eligible src
    LEFT JOIN customers c
        ON src.customer_id = c.id
    WHERE c.city = 'London'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country, device
    FROM with_dims
    INTERSECT
    SELECT country, device
    FROM sessions
    WHERE browser = 'Firefox'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.country = k.country AND s.device = k.device
)
-- Multi-level aggregation
SELECT
    customer_id,
    MAX(row_seq) FILTER (WHERE country = 'DE') AS de_max_row_seq,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    MIN(started_at) AS first_started_at,
    MIN(started_at) AS started_at
FROM kept_rows
GROUP BY 1
ORDER BY span_minutes NULLS LAST;
