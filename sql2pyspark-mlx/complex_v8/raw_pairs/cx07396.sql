WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-05-12' AND DATE '2025-08-07'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(title_len) OVER (PARTITION BY release_date_year ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS title_len_sum_2_prior,
        FIRST_VALUE(release_date_year) IGNORE NULLS OVER (PARTITION BY release_date_year ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_year,
        ROUND(CUME_DIST() OVER (PARTITION BY release_date_year ORDER BY release_date), 3) AS release_date_cume_dist
    FROM cleaned
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(title_len_sum_2_prior / NULLIF(SUM(title_len_sum_2_prior) OVER (PARTITION BY artist_id), 0), 4) AS title_len_sum_2_prior_share_of_artist_id,
        SUM(release_date_cume_dist) OVER (PARTITION BY artist_id) AS artist_id_sum_release_date_cume_dist,
        MAX(bpm) OVER (PARTITION BY title_len, release_date_year ORDER BY release_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_max_3_prior,
        LAG(release_date_cume_dist) OVER (PARTITION BY title_len, release_date_year ORDER BY release_date, id) AS prev_release_date_cume_dist,
        bpm - LAG(bpm) OVER (PARTITION BY title_len, release_date_year ORDER BY release_date, id) AS bpm_change,
        MAX(release_date_year) OVER (PARTITION BY title_len, release_date_year ORDER BY release_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS release_date_year_max_3_prior
    FROM sequenced
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        moods_text,
        title_len_sum_2_prior,
        release_date_cume_dist,
        title_len_sum_2_prior_share_of_artist_id,
        artist_id_sum_release_date_cume_dist,
        prev_release_date_cume_dist,
        bpm_change,
        release_date_year_max_3_prior,
        ROUND(prev_release_date_cume_dist - AVG(prev_release_date_cume_dist) OVER (PARTITION BY release_date_year), 2) AS prev_release_date_cume_dist_vs_release_date_year,
        LAST_VALUE(bpm_change) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY artist_id_sum_release_date_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_change
    FROM sequenced_2
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM windowed s
    LEFT JOIN artists a ON a.id = s.artist_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN title_len_sum_2_prior >= 36 THEN 'large' WHEN title_len_sum_2_prior >= 10 THEN 'medium' ELSE 'small' END AS title_len_sum_2_prior_tier
    FROM with_details_artist
)
SELECT
    id,
    artist_id,
    release_date,
    moods,
    moods_text,
    title_len_sum_2_prior,
    release_date_cume_dist,
    artist_id_sum_release_date_cume_dist,
    prev_release_date_cume_dist_vs_release_date_year,
    last_known_bpm_change,
    title_len_sum_2_prior_tier
FROM tagged
ORDER BY prev_release_date_cume_dist_vs_release_date_year DESC, release_date DESC NULLS FIRST
