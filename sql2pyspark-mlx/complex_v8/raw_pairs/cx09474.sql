WITH valid AS (
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
        CASE WHEN ms_played >= 287769 THEN 'A' WHEN ms_played >= 95960 THEN 'B' ELSE 'C' END AS ms_played_tier,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE source IN ('album', 'search', 'playlist')
),
combined_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        s.days_since_played_at,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year,
        l.signup_date AS listener_signup_date,
        t.title AS track_title
    FROM valid s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(ms_played_tier) OVER (PARTITION BY track_id)) AS distinct_ms_played_tier_in_track_id,
        LAG(listener_signup_date, 2) OVER w_ms_played_tier_by_listener_s AS prev2_listener_signup_date,
        SUM(days_since_played_at) OVER (ORDER BY listener_birth_year, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_played_at_sum_1_prior,
        RANK() OVER (PARTITION BY listener_country, ms_played_tier ORDER BY ms_played) AS ms_played_rank,
        FIRST_VALUE(listener_birth_year) IGNORE NULLS OVER (ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_listener_birth_year,
        SUM(ms_played) OVER (PARTITION BY track_id) AS track_id_sum_ms_played
    FROM combined_details
    WINDOW w_ms_played_tier_by_listener_s AS (PARTITION BY ms_played_tier ORDER BY listener_signup_date, id)
)
SELECT id, measure, amount
FROM (
    SELECT id, ms_played_rank, days_since_played_at, ms_played, distinct_ms_played_tier_in_track_id
    FROM ranked
) src
UNPIVOT (
    amount FOR measure IN (ms_played_rank, days_since_played_at, ms_played, distinct_ms_played_tier_in_track_id)
) unp
ORDER BY id, measure
