WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        MONTH(played_at) AS played_at_month,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-13 09:00:00'
      AND ms_played <= 324058
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.source_clean,
        l.birth_year AS listener_birth_year,
        t.bpm AS track_bpm
    FROM cleaned s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(track_bpm) OVER (ORDER BY listener_birth_year DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS track_bpm_max_2_prior,
        LAST_VALUE(played_at) OVER (ORDER BY listener_birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        LAG(played_at) OVER (PARTITION BY track_id, listener_id ORDER BY track_bpm ASC NULLS LAST, id) AS prev_played_at,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER (PARTITION BY MONTH(played_at)), 0), 4) AS listener_birth_year_share_of_played_at_month,
        SUM(track_bpm) OVER (PARTITION BY listener_id, source) AS listener_id_source_sum_track_bpm,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY listener_birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        FIRST_VALUE(track_bpm) OVER (ORDER BY listener_birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm,
        ROW_NUMBER() OVER (ORDER BY listener_birth_year DESC, id) AS listener_birth_year_seq,
        SUM(track_bpm) OVER (PARTITION BY MONTH(played_at)) AS played_at_month_sum_track_bpm,
        ms_played - LAG(ms_played) OVER w_track_id_listener_id_by_trac AS ms_played_change,
        ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY track_bpm ASC NULLS LAST, id) AS track_bpm_seq,
        RANK() OVER (PARTITION BY track_id ORDER BY ms_played DESC, id) AS ms_played_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY listener_birth_year DESC), 3) AS listener_birth_year_percent_rank
    FROM combined_details_listener
    WINDOW w_track_id_listener_id_by_trac AS (PARTITION BY track_id, listener_id ORDER BY track_bpm ASC NULLS LAST, id)
),
scored AS (
    -- Add calculated columns
    SELECT *, FLOOR(first_track_bpm / 25) * 25 AS first_track_bpm_bucket
    FROM windowed
)
-- Final projection
SELECT DISTINCT track_bpm
FROM scored
ORDER BY track_bpm
