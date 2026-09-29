WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE ms_played < 324437
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        l.signup_date AS listener_signup_date,
        t.bpm AS track_bpm,
        t.title AS track_title
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        listener_signup_date,
        track_bpm,
        track_bpm + ms_played AS track_bpm_plus_ms_played
    FROM with_parent
    WHERE listener_id IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        listener_signup_date,
        track_bpm,
        track_bpm_plus_ms_played,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        COALESCE(ms_played, 0) AS ms_played_filled,
        ABS(track_bpm_plus_ms_played - 260138) AS track_bpm_plus_ms_played_distance
    FROM labeled
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('MONTH', listener_signup_date) AS listener_signup_date_month,
        DATE_FORMAT(played_at_plus_1d, 'yyyy-MM') AS played_at_plus_1d_label
    FROM enriched
    WHERE track_bpm < 127
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_ym,
        TO_DATE(played_at_plus_1d) AS played_at_plus_1d_day,
        TO_DATE(listener_signup_date_month) AS listener_signup_date_month_day,
        MAX(track_bpm_plus_ms_played) AS max_track_bpm_plus_ms_played,
        MAX(ms_played_filled) AS max_ms_played_filled
    FROM enriched_2
    GROUP BY 1, 2, 3
),
derived AS (
    -- Add calculated columns
    SELECT
        listener_signup_date_ym,
        played_at_plus_1d_day,
        listener_signup_date_month_day,
        TRUNC(played_at_plus_1d_day, 'MM') AS played_at_plus_1d_day_month_start,
        SUBSTRING(TRIM(listener_signup_date_ym), 1, 3) AS listener_signup_date_ym_prefix,
        CASE WHEN max_ms_played_filled >= 242000 THEN 'A' WHEN max_ms_played_filled >= 123500 THEN 'B' ELSE 'C' END AS max_ms_played_filled_tier
    FROM rollup_base
    WHERE played_at_plus_1d_day < DATE '2025-03-01'
)
-- Final projection
SELECT
    listener_signup_date_ym,
    played_at_plus_1d_day,
    listener_signup_date_month_day,
    played_at_plus_1d_day_month_start,
    max_ms_played_filled_tier,
    DATE_ADD(played_at_plus_1d_day, 30) AS played_at_plus_1d_day_plus_30d,
    ADD_MONTHS(played_at_plus_1d_day, 3) AS played_at_plus_1d_day_plus_3m
FROM derived
