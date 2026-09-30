WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CONCAT_WS(' / ', source, device) AS source_device_label,
        CASE WHEN source IN ('playlist', 'radio') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE source IN ('playlist', 'album', 'radio')
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.source NOT IN ('playlist', 'album')) FROM plays p WHERE p.listener_id = s.listener_id) AS play_source_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM recent_plays s
),
latest_per_key AS (
    -- Keep the latest row per track_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        source_device_label,
        source_group,
        play_source_count,
        last_played_at,
        first_played_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY first_played_at DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY device, play_source_count ORDER BY last_played_at ASC, id) AS last_played_at_quartile,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        ROUND(ms_played - AVG(ms_played) OVER w_source, 2) AS ms_played_vs_source_avg,
        SUM(ms_played) OVER (PARTITION BY device, play_source_count ORDER BY last_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM latest_per_key
    WINDOW w_source AS (PARTITION BY source)
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source_device_label,
        s.source_group,
        s.play_source_count,
        s.first_played_at,
        s.last_played_at_quartile,
        s.rows_in_source,
        s.ms_played_vs_source_avg,
        s.running_ms_played,
        l.birth_year AS listener_birth_year
    FROM with_trends s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source_device_label,
        s.source_group,
        s.play_source_count,
        s.rows_in_source,
        s.ms_played_vs_source_avg,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year_2,
        l.plan AS listener_plan,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title
    FROM with_details_listener s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source_device_label,
    source_group,
    play_source_count,
    rows_in_source,
    listener_plan,
    track_is_explicit,
    track_title,
    CASE WHEN ms_played_vs_source_avg >= 80000.0 THEN 'large' WHEN ms_played_vs_source_avg >= -240000.0 THEN 'medium' ELSE 'small' END AS ms_played_vs_source_avg_tier,
    INITCAP(TRIM(source_group)) AS source_group_clean
FROM combined_details_listener
WHERE listener_signup_date >= DATE '2024-08-01'
ORDER BY track_title DESC NULLS FIRST, listener_id
