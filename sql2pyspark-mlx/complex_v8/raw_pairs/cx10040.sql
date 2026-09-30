WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        moods,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_id, 0), 4) AS bpm_share_of_artist_id,
        LAG(bpm, 3) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS prev3_bpm,
        LEAD(bpm, 1, 0) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS next_bpm,
        LEAD(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS next_bpm_2,
        RANK() OVER w_artist_id_by_bpm_ties AS bpm_rank,
        MIN(bpm) OVER w_artist_id AS artist_id_min_bpm,
        ROW_NUMBER() OVER w_all_by_bpm AS rn,
        LEAD(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS next2_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY bpm NULLS LAST), 3) AS bpm_percent_rank
    FROM filtered
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_bpm_ties AS (PARTITION BY artist_id ORDER BY bpm), w_all_by_bpm AS (ORDER BY bpm DESC, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        moods,
        bpm_vs_artist_id_avg,
        bpm_share_of_artist_id,
        next_bpm,
        next_bpm_2,
        bpm_rank,
        rn,
        bpm_percent_rank,
        MIN(rn) OVER w_rn AS rn_min_rn,
        ROUND(PERCENT_RANK() OVER w_all_by_next_bpm_2_ties, 3) AS next_bpm_2_percent_rank,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_rn_ties, 3) AS rn_percent_rank,
        ROUND(AVG(next_bpm) OVER w_rn, 2) AS rn_avg_next_bpm
    FROM sequenced
    WINDOW w_rn AS (PARTITION BY rn), w_all_by_next_bpm_2_ties AS (ORDER BY next_bpm_2 NULLS LAST), w_artist_id_by_rn_ties AS (PARTITION BY artist_id ORDER BY rn DESC)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        rn,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(next_bpm_2) / NULLIF(SUM(bpm_vs_artist_id_avg), 0), 3) AS next_bpm_2_to_bpm_vs_artist_id_avg_ratio,
        SUM(bpm_percent_rank) AS total_bpm_percent_rank
    FROM with_windows
    WHERE ARRAY_CONTAINS(moods, 'focus')
    GROUP BY artist_id, rn
)
SELECT
    artist_id,
    distinct_ids,
    next_bpm_2_to_bpm_vs_artist_id_avg_ratio,
    total_bpm_percent_rank,
    ROUND(distinct_ids / NULLIF(total_bpm_percent_rank, 0), 3) AS distinct_ids_per_total_bpm_percent_rank
FROM stats
ORDER BY next_bpm_2_to_bpm_vs_artist_id_avg_ratio DESC
