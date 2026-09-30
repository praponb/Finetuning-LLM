WITH valid_plays AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
),
latest_per_key AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at, id) AS recency_rank
        FROM valid_plays
    ) ranked
    WHERE recency_rank = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM latest_per_key
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.days_since_played_at,
        s.played_at_label,
        l.signup_date AS listener_signup_date
    FROM annotated s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.signup_date >= DATE '2024-04-16'
),
listener_id_stats AS (
    -- Avg days_since_played_at per listener_id
    SELECT listener_id, AVG(days_since_played_at) AS avg_days_since_played_at
    FROM with_parent_listener
    GROUP BY listener_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_played_at,
        ROUND(s.days_since_played_at - g.avg_days_since_played_at, 2) AS days_since_played_at_minus_avg
    FROM with_parent_listener s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.days_since_played_at > g.avg_days_since_played_at
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.days_since_played_at,
        s.played_at_label,
        s.listener_signup_date,
        s.avg_days_since_played_at,
        s.days_since_played_at_minus_avg,
        l.interests AS listener_interests
    FROM outliers s
    JOIN listeners l ON l.id = s.listener_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', listener_signup_date) AS listener_signup_date_month,
        COUNT(*) FILTER (WHERE source = 'playlist') AS n_source_playlist,
        ROUND(SUM(days_since_played_at) / NULLIF(SUM(avg_days_since_played_at), 0), 3) AS days_since_played_at_to_avg_days_since_played_at,
        COUNT(*) AS n_rows,
        ROUND(SUM(avg_days_since_played_at) / NULLIF(SUM(ms_played), 0), 3) AS avg_days_since_played_at_to_ms_played_ratio,
        MIN(days_since_played_at_minus_avg) AS min_days_since_played_at_minus_avg
    FROM joined_listener
    GROUP BY DATE_TRUNC('MONTH', listener_signup_date)
)
SELECT
    listener_signup_date_month,
    avg_days_since_played_at_to_ms_played_ratio,
    min_days_since_played_at_minus_avg,
    CASE WHEN n_source_playlist >= 6 THEN 'large' WHEN n_source_playlist >= 4 THEN 'medium' ELSE 'small' END AS n_source_playlist_tier
FROM rollup_base
WHERE n_source_playlist <= 6
ORDER BY avg_days_since_played_at_to_ms_played_ratio, listener_signup_date_month
LIMIT 5
