WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(page) AS page_length,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-08-31 17:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page_clean,
        s.island_id,
        p.device,
        p.country
    FROM sessionized s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.started_at < '2025-03-01'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.country AS session_country,
        p.browser
    FROM joined a
    INNER JOIN sessions p
        ON a.session_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(island_id) OVER w_ordered AS prev_island_id,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY event_type ORDER BY island_id, id)
)
-- Multi-level aggregation
SELECT
    session_country,
    COUNT(DISTINCT session_country) AS distinct_session_countries,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50
FROM scored
GROUP BY session_country
ORDER BY distinct_session_countries;
