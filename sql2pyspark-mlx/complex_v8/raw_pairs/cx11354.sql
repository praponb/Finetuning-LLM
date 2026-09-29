WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE release_date < DATE '2024-12-22'
      AND is_explicit = FALSE
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM filtered s
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('|', title, title_prefix) AS title_title_prefix_label,
        CASE WHEN bpm_filled >= 150 THEN 'A' WHEN bpm_filled >= 90 THEN 'B' ELSE 'C' END AS bpm_filled_tier,
        LEAST(play_count, bpm) AS least_play_count_bpm
    FROM with_counts
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        title_prefix,
        greatest_duration_sec_bpm,
        play_count,
        ROUND(AVG(bpm) OVER (PARTITION BY play_count ORDER BY least_play_count_bpm, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        ROW_NUMBER() OVER w_all_by_greatest_duration_sec AS seq_no,
        LEAD(bpm_filled, 2) OVER (PARTITION BY play_count ORDER BY least_play_count_bpm, id) AS next2_bpm_filled
    FROM tagged
    WINDOW w_all_by_greatest_duration_sec AS (ORDER BY greatest_duration_sec_bpm ASC NULLS LAST, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(seq_no - AVG(seq_no) OVER w_artist_id, 2) AS seq_no_vs_artist_id_avg,
        LAG(greatest_duration_sec_bpm, 1, 0) OVER (ORDER BY seq_no DESC, id) AS prev_greatest_duration_sec_bpm,
        seq_no - LAG(seq_no) OVER (ORDER BY seq_no DESC, id) AS seq_no_change,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm_avg_2_prior NULLS LAST) AS bpm_avg_2_prior_dense_rank
    FROM ranked
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(title_prefix), ' ', '_')) AS title_prefix_slug,
        ROUND(play_count / NULLIF(next2_bpm_filled, 0), 3) AS play_count_per_next2_bpm_filled
    FROM sequenced
),
artist_id_stats AS (
    -- Median prev_greatest_duration_sec_bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(prev_greatest_duration_sec_bpm, 0.5) AS median_prev_greatest_duration_sec_bpm
    FROM tagged_2
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prev_greatest_duration_sec_bpm,
        ROUND(s.prev_greatest_duration_sec_bpm - g.median_prev_greatest_duration_sec_bpm, 2) AS prev_greatest_duration_sec_bpm_minus_median
    FROM tagged_2 s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.prev_greatest_duration_sec_bpm >= g.median_prev_greatest_duration_sec_bpm
)
-- Final projection
SELECT
    id,
    is_explicit,
    title_prefix,
    greatest_duration_sec_bpm,
    play_count,
    bpm_avg_2_prior,
    seq_no,
    next2_bpm_filled,
    seq_no_vs_artist_id_avg,
    seq_no_change,
    bpm_avg_2_prior_dense_rank,
    play_count_per_next2_bpm_filled,
    median_prev_greatest_duration_sec_bpm,
    FLOOR(next2_bpm_filled / 25) * 25 AS next2_bpm_filled_bucket
FROM vs_group
WHERE play_count < 7
