WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 12:00:00'
),
stats AS (
    -- Summarise per group
    SELECT
        listener_id,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(ms_played_bucket), 2) AS stddev_ms_played_bucket,
        COUNT(*) AS n_rows,
        MAX(played_at) AS last_played_at,
        MIN(played_at) AS first_played_at
    FROM cleaned
    GROUP BY 1
),
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.listener_id,
        s.stddev_ms_played_bucket,
        s.n_rows,
        s.last_played_at,
        s.first_played_at,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM stats s
    JOIN listeners l ON l.id = s.listener_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year
    FROM with_parent_listener s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    listener_id,
    stddev_ms_played_bucket,
    last_played_at,
    first_played_at,
    listener_plan,
    listener_interests,
    listener_signup_date,
    listener_birth_year,
    ROUND(stddev_ms_played_bucket * 1.5, 2) AS stddev_ms_played_bucket_adj,
    SUBSTRING(TRIM(listener_plan), 1, 3) AS listener_plan_prefix
FROM combined_details
ORDER BY listener_plan_prefix
