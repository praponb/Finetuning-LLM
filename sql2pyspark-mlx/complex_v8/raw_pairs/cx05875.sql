WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE release_date < DATE '2025-03-08'
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        a.country AS artist_country
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.genre IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        moods,
        DENSE_RANK() OVER w_artist_id_by_duration_sec_ti AS duration_sec_dense_rank,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY bpm ASC) AS bpm_dense_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_country, artist_id), 2) AS artist_country_artist_id_avg_bpm,
        DENSE_RANK() OVER w_all_by_duration_sec_ties AS duration_sec_dense_rank_2,
        LEAD(duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec, id) AS next2_duration_sec,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        COUNT(*) OVER (PARTITION BY artist_country, artist_id) AS rows_in_artist_country_artist_id,
        NTILE(3) OVER (ORDER BY release_date, id) AS release_date_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec), 3) AS duration_sec_percent_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_country, artist_id), 0), 4) AS duration_sec_share_of_artist_country_artist_id,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date
    FROM with_details_artist
    WINDOW w_artist_id_by_duration_sec_ti AS (PARTITION BY artist_id ORDER BY duration_sec), w_all_by_duration_sec_ties AS (ORDER BY duration_sec DESC)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        duration_sec,
        moods,
        rows_in_artist_id,
        bpm_dense_rank,
        artist_country_artist_id_avg_bpm,
        duration_sec_dense_rank_2,
        next2_duration_sec,
        first_bpm,
        duration_sec_share_of_artist_id,
        rows_in_artist_country_artist_id,
        release_date_tercile,
        duration_sec_percent_rank,
        duration_sec_share_of_artist_country_artist_id,
        second_release_date
    FROM ranked
    WHERE duration_sec_dense_rank <= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        moods,
        rows_in_artist_id,
        bpm_dense_rank,
        next2_duration_sec,
        first_bpm,
        release_date_tercile,
        duration_sec_percent_rank,
        duration_sec_share_of_artist_country_artist_id,
        second_release_date,
        MAX(duration_sec_dense_rank_2) OVER (PARTITION BY duration_sec_dense_rank_2 ORDER BY first_bpm, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_dense_rank_2_max_3_prior,
        ROUND(duration_sec_dense_rank_2 / NULLIF(SUM(duration_sec_dense_rank_2) OVER (PARTITION BY bpm_dense_rank, release_date_tercile), 0), 4) AS duration_sec_dense_rank_2_share_of_bpm_dense_ran,
        duration_sec_percent_rank - LAG(duration_sec_percent_rank) OVER (PARTITION BY duration_sec_dense_rank_2 ORDER BY first_bpm, id) AS duration_sec_percent_rank_change,
        ROUND(AVG(release_date_tercile) OVER (PARTITION BY bpm_dense_rank, release_date_tercile), 2) AS bpm_dense_rank_release_date_tercile_avg_release_
    FROM leaders
)
SELECT
    id,
    moods,
    rows_in_artist_id,
    bpm_dense_rank,
    first_bpm,
    release_date_tercile,
    duration_sec_percent_rank,
    duration_sec_share_of_artist_country_artist_id,
    second_release_date,
    duration_sec_dense_rank_2_max_3_prior,
    bpm_dense_rank_release_date_tercile_avg_release_,
    DATE_FORMAT(second_release_date, 'MMM yyyy') AS second_release_date_label,
    DATE_FORMAT(second_release_date, 'MMM yyyy') AS second_release_date_label_2
FROM windowed
WHERE rows_in_artist_id > 4
ORDER BY duration_sec_dense_rank_2_max_3_prior DESC, second_release_date_label
