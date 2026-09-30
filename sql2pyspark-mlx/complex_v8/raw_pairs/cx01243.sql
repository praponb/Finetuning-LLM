WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND is_explicit = TRUE
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.title_clean,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY artist_debut_year ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_sum_7,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_duration_sec, 3) AS duration_sec_percent_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_country, artist_id), 0), 4) AS bpm_share_of_artist_country_artist_id,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY artist_debut_year ASC) AS artist_debut_year_dense_rank,
        MAX(bpm) OVER (PARTITION BY artist_country ORDER BY bpm NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS bpm_max_2_prior,
        bpm - LAG(bpm) OVER (PARTITION BY artist_country ORDER BY bpm NULLS LAST, id) AS bpm_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY artist_debut_year ASC), 3) AS artist_debut_year_percent_rank,
        LEAD(bpm, 3) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id) AS next3_bpm,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm
    FROM joined
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec ASC, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_trends
    WHERE artist_debut_year_dense_rank <= 3
)
SELECT
    id,
    title,
    duration_sec,
    release_date,
    moods,
    title_clean,
    artist_debut_year,
    artist_country,
    bpm_sum_7,
    bpm_share_of_artist_country_artist_id,
    artist_debut_year_dense_rank,
    bpm_change,
    artist_debut_year_percent_rank,
    next3_bpm,
    first_bpm,
    CONCAT_WS('-', title, title_clean) AS title_title_clean_label
FROM top_rows
WHERE artist_id IS NOT NULL
ORDER BY artist_debut_year_percent_rank, id
LIMIT 5
