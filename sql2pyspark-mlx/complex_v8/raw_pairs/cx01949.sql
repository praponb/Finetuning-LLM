WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        DATE_TRUNC('WEEK', played_at) AS played_at_week
    FROM plays
),
device_stats AS (
    -- Avg ms_played per device
    SELECT device, AVG(ms_played) AS avg_ms_played
    FROM scoped_plays
    GROUP BY device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM scoped_plays s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played > g.avg_ms_played
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ms_played_minus_avg) OVER (PARTITION BY is_skipped_label), 2) AS is_skipped_label_avg_ms_played_minus_avg,
        ROUND(avg_ms_played / NULLIF(SUM(avg_ms_played) OVER w_is_skipped_label, 0), 4) AS avg_ms_played_share_of_is_skipped_label,
        MAX(ms_played_minus_avg) OVER w_is_skipped_label AS is_skipped_label_max_ms_played_minus_avg,
        MAX(ms_played_minus_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at_week) ORDER BY avg_ms_played, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_minus_avg_max_3
    FROM vs_group
    WINDOW w_is_skipped_label AS (PARTITION BY is_skipped_label)
),
joined AS (
    -- Join listeners
    SELECT
        s.*,
        l.birth_year AS listener_birth_year,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date
    FROM with_windows s
    JOIN listeners l ON l.id = s.listener_id
    WHERE ARRAY_CONTAINS(l.interests, 'indie')
),
joined_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.is_skipped_label,
        s.played_at_week,
        s.avg_ms_played,
        s.ms_played_minus_avg,
        s.is_skipped_label_avg_ms_played_minus_avg,
        s.avg_ms_played_share_of_is_skipped_label,
        s.ms_played_minus_avg_max_3,
        s.listener_birth_year,
        s.listener_interests,
        s.listener_signup_date,
        l.signup_date AS listener_signup_date_2,
        l.plan AS listener_plan
    FROM joined s
    JOIN listeners l ON l.id = s.listener_id
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.artist_id AS track_artist_id
    FROM joined_2 s
    JOIN tracks t ON t.id = s.track_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(ms_played_minus_avg) OVER (PARTITION BY source, YEAR(played_at_week)) AS source_played_at_week_year_max_ms_played_minus_a,
        ROUND(PERCENT_RANK() OVER w_all_by_listener_signup_date, 3) AS listener_signup_date_percent_rank,
        COUNT(*) OVER (PARTITION BY source, YEAR(played_at_week)) AS rows_in_source_played_at_week_year,
        MIN(ms_played_minus_avg_max_3) OVER (ORDER BY listener_signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_minus_avg_max_3_so_far,
        MAX(avg_ms_played) OVER (PARTITION BY source, YEAR(played_at_week)) AS source_played_at_week_year_max_avg_ms_played,
        LAG(avg_ms_played, 1, 0) OVER (ORDER BY listener_signup_date ASC, id) AS prev_avg_ms_played
    FROM with_parent_track
    WINDOW w_all_by_listener_signup_date AS (ORDER BY listener_signup_date ASC, id)
)
SELECT
    id,
    track_id,
    ms_played,
    device,
    is_skipped,
    source,
    is_skipped_label,
    avg_ms_played,
    ms_played_minus_avg,
    avg_ms_played_share_of_is_skipped_label,
    ms_played_minus_avg_max_3,
    listener_birth_year,
    listener_interests,
    listener_signup_date,
    listener_signup_date_2,
    listener_plan,
    source_played_at_week_year_max_ms_played_minus_a,
    rows_in_source_played_at_week_year,
    min_ms_played_minus_avg_max_3_so_far,
    source_played_at_week_year_max_avg_ms_played,
    prev_avg_ms_played,
    DATEDIFF(DATE '2025-06-30', played_at_week) AS days_since_played_at_week
FROM sequenced
ORDER BY ms_played_minus_avg_max_3 DESC
