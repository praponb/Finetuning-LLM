WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 5
      AND is_explicit = TRUE
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM cleaned_tracks s
),
joined_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.country AS artist_country
    FROM summarized_rows s
    LEFT JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        track_count,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id, artist_country ORDER BY track_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        SUM(track_count) OVER (PARTITION BY track_count, artist_id ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS track_count_sum_1_prior,
        RANK() OVER (PARTITION BY artist_id, artist_country ORDER BY track_count) AS track_count_rank,
        ROUND(track_count / NULLIF(SUM(track_count) OVER w_is_explicit_release_date_mon, 0), 4) AS track_count_share_of_is_explicit_release_date_mo,
        ROUND(PERCENT_RANK() OVER w_track_count_artist_id_by_rel_2, 3) AS release_date_percent_rank
    FROM joined_artist
    WINDOW w_is_explicit_release_date_mon AS (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date)), w_track_count_artist_id_by_rel_2 AS (PARTITION BY track_count, artist_id ORDER BY release_date DESC, id)
)
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    track_count,
    first_release_date,
    track_count_sum_1_prior,
    release_date_percent_rank,
    MONTH(release_date) AS release_date_month
FROM ranked
WHERE is_explicit = TRUE
ORDER BY track_count_sum_1_prior DESC NULLS FIRST
