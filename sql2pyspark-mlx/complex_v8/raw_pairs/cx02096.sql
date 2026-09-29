WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE (SIZE(moods) >= 2 OR duration_sec < 365)
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.debut_year AS artist_debut_year, a.artist_name
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        release_date,
        artist_name,
        LAG(release_date) OVER (PARTITION BY artist_id ORDER BY artist_debut_year NULLS LAST, id) AS prev_release_date,
        ROUND(AVG(moods_count) OVER (PARTITION BY artist_id ORDER BY artist_debut_year NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS moods_count_avg_2,
        SUM(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY artist_debut_year NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS artist_debut_year_sum_3_prior,
        LAG(release_date) OVER w_all_by_artist_debut_year AS prev_release_date_2,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER w_moods_count_artist_id, 0), 4) AS artist_debut_year_share_of_moods_count_artist_id
    FROM with_parent_artist
    WINDOW w_all_by_artist_debut_year AS (ORDER BY artist_debut_year ASC, id), w_moods_count_artist_id AS (PARTITION BY moods_count, artist_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER w_all_by_artist_debut_year_sha AS artist_debut_year_share_of_moods_count_artist_id_2,
        ROUND(moods_count_avg_2 - AVG(moods_count_avg_2) OVER w_artist_id, 2) AS moods_count_avg_2_vs_artist_id_avg,
        DATEDIFF(prev_release_date_2, LAG(prev_release_date_2) OVER w_all_by_artist_debut_year_sha) AS days_since_prev_prev_release_date_2,
        LAG(prev_release_date_2, 3) OVER (ORDER BY artist_debut_year_share_of_moods_count_artist_id DESC, id) AS prev3_prev_release_date_2,
        ROW_NUMBER() OVER (ORDER BY artist_debut_year_share_of_moods_count_artist_id DESC, id) AS rn,
        NTH_VALUE(moods_count_avg_2, 2) OVER (ORDER BY artist_debut_year_share_of_moods_count_artist_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moods_count_avg_2
    FROM ranked
    WINDOW w_all_by_artist_debut_year_sha AS (ORDER BY artist_debut_year_share_of_moods_count_artist_id DESC, id), w_artist_id AS (PARTITION BY artist_id)
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        artist_name,
        prev_release_date,
        moods_count_avg_2,
        artist_debut_year_sum_3_prior,
        prev_release_date_2,
        artist_debut_year_share_of_moods_count_artist_id,
        artist_debut_year_share_of_moods_count_artist_id_2,
        moods_count_avg_2_vs_artist_id_avg,
        days_since_prev_prev_release_date_2,
        prev3_prev_release_date_2,
        rn,
        second_moods_count_avg_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY days_since_prev_prev_release_date_2 DESC, id DESC) AS row_num
        FROM with_trends
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        TRUNC(prev_release_date, 'MM') AS prev_release_date_month_start,
        DATEDIFF(prev_release_date, release_date) AS days_release_date_to_prev_release_date,
        CONCAT_WS('|', artist_name, title) AS artist_name_title_label
    FROM unique_rows
    WHERE moods_count_avg_2 >= 1.2
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        release_date,
        artist_name,
        prev_release_date,
        moods_count_avg_2,
        artist_debut_year_sum_3_prior,
        prev_release_date_2,
        artist_debut_year_share_of_moods_count_artist_id,
        artist_debut_year_share_of_moods_count_artist_id_2,
        moods_count_avg_2_vs_artist_id_avg,
        days_since_prev_prev_release_date_2,
        prev3_prev_release_date_2,
        second_moods_count_avg_2,
        prev_release_date_month_start,
        days_release_date_to_prev_release_date,
        artist_name_title_label
    FROM derived
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    artist_name,
    prev_release_date,
    moods_count_avg_2,
    prev_release_date_2,
    artist_debut_year_share_of_moods_count_artist_id,
    moods_count_avg_2_vs_artist_id_avg,
    prev3_prev_release_date_2,
    second_moods_count_avg_2,
    prev_release_date_month_start,
    days_release_date_to_prev_release_date,
    artist_name_title_label,
    ROUND(second_moods_count_avg_2 * 0.9, 2) AS second_moods_count_avg_2_adj,
    YEAR(prev_release_date) AS prev_release_date_year
FROM first_rows
WHERE artist_name_title_label IS NOT NULL
ORDER BY title NULLS LAST, id
LIMIT 10
