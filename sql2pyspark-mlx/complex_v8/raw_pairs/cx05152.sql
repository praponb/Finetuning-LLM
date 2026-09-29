WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        LENGTH(TRIM(title)) AS title_len,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
      AND artist_id <= 6
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY title_len)) AS distinct_artist_id_in_title_len,
        NTILE(5) OVER w_all_by_moods_count AS moods_count_quintile,
        ROUND(title_len - AVG(title_len) OVER (PARTITION BY title_len), 2) AS title_len_vs_title_len_avg,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY title_len), 2) AS bpm_vs_title_len_avg
    FROM cleaned
    WINDOW w_all_by_moods_count AS (ORDER BY moods_count, id)
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.moods_count,
        s.title_len,
        s.release_date_label,
        s.distinct_artist_id_in_title_len,
        s.moods_count_quintile,
        s.title_len_vs_title_len_avg,
        s.bpm_vs_title_len_avg,
        a.is_verified AS artist_is_verified
    FROM sequenced s
    JOIN artists a ON a.id = s.artist_id
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        *,
        bpm_vs_title_len_avg - moods_count AS bpm_vs_title_len_avg_minus_moods_count,
        LENGTH(TRIM(release_date_label)) AS release_date_label_len,
        COALESCE(bpm, 0) AS bpm_filled
    FROM enriched
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.moods_count,
        s.release_date_label,
        s.distinct_artist_id_in_title_len,
        s.moods_count_quintile,
        s.artist_is_verified,
        s.bpm_vs_title_len_avg_minus_moods_count,
        s.release_date_label_len,
        a.is_verified AS artist_is_verified_2,
        a.country AS artist_country,
        a.artist_name
    FROM enriched_2 s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT id, field, metric_value
FROM (
    SELECT id, release_date_label_len, moods_count_quintile, moods_count, duration_sec
    FROM enriched_artist
) src
UNPIVOT (
    metric_value FOR field IN (release_date_label_len, moods_count_quintile, moods_count, duration_sec)
) unp
ORDER BY id, field
