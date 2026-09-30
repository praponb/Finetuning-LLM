WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND LOWER(title) LIKE '%he%'
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.moods,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM cleaned_tracks s
    JOIN plays p ON p.track_id = s.id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        play_is_skipped,
        COUNT(*) FILTER (WHERE play_is_skipped = FALSE) AS n_play_is_skipped_false,
        SUM(bpm) AS total_bpm
    FROM detail_rows_plays
    GROUP BY play_is_skipped
),
labeled AS (
    -- Derive helper columns
    SELECT
        play_is_skipped,
        n_play_is_skipped_false,
        total_bpm,
        ROUND(n_play_is_skipped_false / NULLIF(total_bpm, 0), 3) AS n_play_is_skipped_false_per_total_bpm,
        CASE WHEN total_bpm >= 825 THEN 'HIGH' WHEN total_bpm >= 468 THEN 'MEDIUM' ELSE 'LOW' END AS total_bpm_tier,
        ROUND(n_play_is_skipped_false / NULLIF(total_bpm, 0), 3) AS n_play_is_skipped_false_per_total_bpm_2
    FROM rollup_base
),
tagged AS (
    -- Add calculated columns
    SELECT
        play_is_skipped,
        n_play_is_skipped_false,
        total_bpm,
        total_bpm_tier,
        n_play_is_skipped_false_per_total_bpm_2,
        n_play_is_skipped_false + total_bpm AS n_play_is_skipped_false_plus_total_bpm,
        ABS(total_bpm - 519) AS total_bpm_distance,
        INITCAP(TRIM(total_bpm_tier)) AS total_bpm_tier_clean
    FROM labeled
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_play_is_skipped_by_total_bpm, 3) AS total_bpm_percent_rank,
        ROUND(total_bpm_distance / NULLIF(SUM(total_bpm_distance) OVER w_total_bpm_tier, 0), 4) AS total_bpm_distance_share_of_total_bpm_tier,
        COUNT(*) OVER (PARTITION BY total_bpm_tier) AS rows_in_total_bpm_tier
    FROM tagged
    WINDOW w_play_is_skipped_by_total_bpm AS (PARTITION BY play_is_skipped ORDER BY total_bpm ASC), w_total_bpm_tier AS (PARTITION BY total_bpm_tier)
)
SELECT play_is_skipped, metric, reading
FROM (
    SELECT play_is_skipped, total_bpm_percent_rank, total_bpm_distance_share_of_total_bpm_tier, n_play_is_skipped_false_per_total_bpm_2
    FROM ranked
) src
UNPIVOT (
    reading FOR metric IN (total_bpm_percent_rank AS total_bpm_percent_rank_v, total_bpm_distance_share_of_total_bpm_tier AS total_bpm_distance_share_of_total_bpm_tier_v, n_play_is_skipped_false_per_total_bpm_2 AS n_play_is_skipped_false_per_total_bpm_2_v)
) unp
