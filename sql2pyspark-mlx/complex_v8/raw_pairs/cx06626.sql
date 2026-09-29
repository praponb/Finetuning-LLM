WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE track_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER (PARTITION BY track_id), 0), 4) AS ms_played_filled_share_of_track_id,
        SUM(ms_played) OVER (PARTITION BY track_id) AS track_id_sum_ms_played,
        RANK() OVER w_played_at_to_date_track_id_b AS ms_played_rank
    FROM base
    WINDOW w_played_at_to_date_track_id_b AS (PARTITION BY TO_DATE(played_at), track_id ORDER BY ms_played)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE ms_played_rank = 1
      AND ms_played <= 324058
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        MIN(played_at) AS first_played_at,
        ROUND(AVG(ms_played_filled_share_of_track_id), 2) AS avg_ms_played_filled_share_of_track_id,
        MAX(played_at) AS last_played_at
    FROM leaders
    GROUP BY DATE_TRUNC('MONTH', played_at)
)
SELECT
    played_at_month,
    first_played_at,
    last_played_at,
    DATE_TRUNC('QUARTER', last_played_at) AS last_played_at_quarter
FROM totals
ORDER BY played_at_month, last_played_at DESC NULLS FIRST
