WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(country) AS country_length
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-30 07:00:00'
      AND browser IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.email,
        b.name
    FROM sessions_sessions e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        is_new_session,
        island_id,
        email,
        island_id_2,
        NTILE(3) OVER (PARTITION BY customer_id, device ORDER BY island_id_2 DESC, id) AS island_id_2_bucket,
        SUM(island_id_2) OVER (ORDER BY country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_island_id_2,
        ROW_NUMBER() OVER (ORDER BY island_id, id) AS row_seq
    FROM islands
    WHERE name LIKE '%smith%'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY cumulative_island_id_2 DESC NULLS LAST, id) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.island_id,
    s.island_id_2_bucket,
    s.cumulative_island_id_2,
    s.row_seq,
    s.row_num,
    (SELECT AVG(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS avg_order_amount
FROM deduped s
ORDER BY row_seq DESC NULLS FIRST;
