WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'S%' OR duration_sec < 352)
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS min_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM prepared_tracks s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        moods_count,
        bpm_minus_duration_sec,
        min_play_ms_played,
        NTH_VALUE(min_play_ms_played, 2) OVER (PARTITION BY moods_count ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_play_ms_played,
        ROUND(play_count - AVG(play_count) OVER (PARTITION BY artist_id), 2) AS play_count_vs_artist_id_avg,
        MAX(bpm_minus_duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_bpm_minus_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        SUM(bpm) OVER (PARTITION BY moods_count ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm
    FROM with_counts
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm_minus_duration_sec DESC NULLS FIRST) AS bpm_minus_duration_sec_dense_rank,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm_minus_duration_sec DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS bpm_max_2_prior,
        NTILE(4) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm_minus_duration_sec DESC NULLS FIRST, id) AS bpm_minus_duration_sec_quartile,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        NTH_VALUE(rows_in_artist_id, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm_minus_duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_artist_id
    FROM sequenced
),
artist_id_stats AS (
    -- Median rows_in_artist_id per artist_id
    SELECT artist_id, PERCENTILE_APPROX(rows_in_artist_id, 0.5) AS median_rows_in_artist_id
    FROM windowed
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_rows_in_artist_id,
        ROUND(s.rows_in_artist_id - g.median_rows_in_artist_id, 2) AS rows_in_artist_id_minus_median
    FROM windowed s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.rows_in_artist_id >= g.median_rows_in_artist_id
),
totals AS (
    -- Group-level totals
    SELECT
        CASE WHEN bpm_max_2_prior >= 420 THEN 'large' WHEN bpm_max_2_prior >= 252 THEN 'medium' ELSE 'small' END AS bpm_max_2_prior_tier,
        artist_id,
        bpm_minus_duration_sec_quartile,
        SUM(rows_in_artist_id_minus_median) AS total_rows_in_artist_id_minus_median,
        COUNT(*) AS cnt,
        ROUND(AVG(second_rows_in_artist_id), 2) AS avg_second_rows_in_artist_id
    FROM compared
    GROUP BY CASE WHEN bpm_max_2_prior >= 420 THEN 'large' WHEN bpm_max_2_prior >= 252 THEN 'medium' ELSE 'small' END, artist_id, bpm_minus_duration_sec_quartile
)
SELECT bpm_max_2_prior_tier, artist_id, field, amount
FROM (
    SELECT bpm_max_2_prior_tier, artist_id, avg_second_rows_in_artist_id, total_rows_in_artist_id_minus_median
    FROM totals
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (avg_second_rows_in_artist_id, total_rows_in_artist_id_minus_median)
) unp
