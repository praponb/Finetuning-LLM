WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec < 402
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec - LAG(duration_sec) OVER (ORDER BY bpm NULLS LAST, id) AS duration_sec_change,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm, id) AS rn,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id, 2) AS duration_sec_vs_artist_id_avg,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm, 3) AS bpm_percent_rank,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS duration_sec_change_2,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        SUM(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_2,
        RANK() OVER (ORDER BY bpm NULLS LAST) AS bpm_rank
    FROM filtered
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_all_by_bpm AS (ORDER BY bpm NULLS LAST, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY rn) AS rows_in_rn,
        RANK() OVER w_is_explicit_bpm_rank_by_dura AS duration_sec_vs_artist_id_avg_rank,
        ROUND(duration_sec_change / NULLIF(SUM(duration_sec_change) OVER (PARTITION BY rn), 0), 4) AS duration_sec_change_share_of_rn,
        NTH_VALUE(artist_id_max_bpm, 2) OVER (PARTITION BY is_explicit, bpm_rank ORDER BY duration_sec_vs_artist_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_id_max_bpm,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY rn), 0), 4) AS duration_sec_share_of_rn,
        bpm_rank - LAG(bpm_rank) OVER (PARTITION BY is_explicit, bpm_rank ORDER BY duration_sec_vs_artist_id_avg DESC, id) AS bpm_rank_change
    FROM ranked
    WINDOW w_is_explicit_bpm_rank_by_dura AS (PARTITION BY is_explicit, bpm_rank ORDER BY duration_sec_vs_artist_id_avg DESC)
)
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    bpm,
    duration_sec_change,
    duration_sec_vs_artist_id_avg,
    bpm_share_of_artist_id,
    rows_in_is_explicit_artist_id,
    bpm_percent_rank,
    bpm_rank,
    rows_in_rn,
    second_artist_id_max_bpm,
    duration_sec_share_of_rn,
    bpm_rank_change,
    SUBSTRING(TRIM(title), 1, 2) AS title_prefix
FROM metrics
ORDER BY bpm DESC, duration_sec
