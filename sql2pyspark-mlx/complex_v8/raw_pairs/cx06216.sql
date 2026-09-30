WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
      AND release_date < DATE '2025-03-08'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        title_len,
        moods_count,
        MIN(bpm) OVER w_artist_id_moods_count AS artist_id_moods_count_min_bpm,
        ROUND(AVG(title_len) OVER (ORDER BY title_len, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS title_len_avg_6_prior,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, moods_count), 2) AS bpm_vs_artist_id_moods_count_avg,
        DATEDIFF(release_date, LAG(release_date) OVER w_all_by_title_len) AS days_since_prev_release_date
    FROM cleaned_tracks
    WINDOW w_artist_id_moods_count AS (PARTITION BY artist_id, moods_count), w_all_by_title_len AS (ORDER BY title_len, id)
)
SELECT id, measure, reading
FROM (
    SELECT id, title_len_avg_6_prior, bpm_vs_artist_id_moods_count_avg
    FROM sequenced
) src
UNPIVOT (
    reading FOR measure IN (title_len_avg_6_prior AS title_len_avg_6_prior_v, bpm_vs_artist_id_moods_count_avg AS m_bpm_vs_artist_id_moods_count_avg)
) unp
