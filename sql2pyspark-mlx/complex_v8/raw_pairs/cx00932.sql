WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref
    FROM plays
    WHERE source <> 'search'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        NTILE(10) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id) AS ms_played_decile,
        LAG(ms_played, 3) OVER (PARTITION BY track_id, source ORDER BY played_at DESC NULLS FIRST, id) AS prev3_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank
    FROM scoped
),
listener_id_stats AS (
    -- Avg ms_played per listener_id
    SELECT listener_id, AVG(ms_played) AS avg_ms_played
    FROM metrics
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM metrics s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played > g.avg_ms_played
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY ms_played_decile, listener_id ORDER BY prev3_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        avg_ms_played - LAG(avg_ms_played) OVER (PARTITION BY ms_played_decile, listener_id ORDER BY prev3_ms_played, id) AS avg_ms_played_change,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        ms_played_percent_rank - LAG(ms_played_percent_rank) OVER (PARTITION BY is_skipped, MONTH(played_at) ORDER BY prev3_ms_played NULLS LAST, id) AS ms_played_percent_rank_change,
        LAST_VALUE(ms_played) OVER (PARTITION BY ms_played_decile, listener_id ORDER BY prev3_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played
    FROM above_average
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.birth_year AS listener_birth_year,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username
    FROM ranked s
    JOIN listeners l ON l.id = s.listener_id
),
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.ms_played_decile,
        s.prev3_ms_played,
        s.ms_played_minus_avg,
        s.first_known_played_at,
        s.rows_in_listener_id,
        s.ms_played_percent_rank_change,
        s.last_ms_played,
        s.listener_signup_date,
        s.listener_username,
        l.birth_year AS listener_birth_year_2
    FROM combined_details s
    JOIN listeners l ON l.id = s.listener_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        ms_played_decile,
        prev3_ms_played,
        first_known_played_at,
        rows_in_listener_id,
        ms_played_percent_rank_change,
        last_ms_played,
        listener_birth_year_2,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        ABS(listener_birth_year_2 - 1975) AS listener_birth_year_2_distance
    FROM with_parent_listener
    WHERE last_ms_played < 302279
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    ms_played_decile,
    prev3_ms_played,
    first_known_played_at,
    rows_in_listener_id,
    last_ms_played,
    listener_birth_year_2,
    listener_birth_year_2_distance
FROM derived
ORDER BY ms_played_decile DESC, prev3_ms_played DESC, id
LIMIT 20
