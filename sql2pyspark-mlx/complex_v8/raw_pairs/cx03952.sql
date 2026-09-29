WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        CASE WHEN duration_sec >= 352 THEN 'HIGH' WHEN duration_sec >= 164 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE duration_sec BETWEEN 144 AND 357
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.moods_count,
        a.artist_name,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(moods_count) OVER (PARTITION BY moods_count, artist_id ORDER BY moods_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_moods_count_so_far,
        RANK() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY moods_count) AS moods_count_rank,
        NTH_VALUE(moods_count, 2) OVER (PARTITION BY artist_genre ORDER BY moods_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moods_count,
        LAG(duration_sec, 2) OVER (PARTITION BY moods_count, artist_id ORDER BY moods_count NULLS LAST, id) AS prev2_duration_sec,
        MAX(moods_count) OVER (ORDER BY moods_count DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS moods_count_max_2_prior,
        SUM(moods_count) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_sum_moods_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY moods_count), 3) AS moods_count_percent_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_is_verified) AS rows_in_artist_is_verified
    FROM with_parent_artist
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_artist_is_verified_by_moods AS moods_count_percent_rank_rank,
        LAST_VALUE(rows_in_artist_is_verified) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY moods_count_percent_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rows_in_artist_is_verified,
        COUNT(*) OVER (PARTITION BY artist_genre, moods_count_rank) AS rows_in_artist_genre_moods_count_rank,
        ROUND(second_moods_count / NULLIF(SUM(second_moods_count) OVER (PARTITION BY artist_genre, moods_count_rank), 0), 4) AS second_moods_count_share_of_artist_genre_moods_c,
        LAST_VALUE(moods_count_rank) OVER (PARTITION BY second_moods_count ORDER BY moods_count_percent_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moods_count_rank
    FROM windowed
    WINDOW w_artist_is_verified_by_moods AS (PARTITION BY artist_is_verified ORDER BY moods_count_percent_rank NULLS LAST, id)
)
SELECT
    id,
    artist_id,
    artist_name,
    artist_is_verified,
    artist_genre,
    min_moods_count_so_far,
    moods_count_rank,
    second_moods_count,
    prev2_duration_sec,
    moods_count_max_2_prior,
    artist_is_verified_sum_moods_count,
    moods_count_percent_rank,
    first_known_duration_sec,
    rows_in_artist_is_verified,
    moods_count_percent_rank_rank,
    last_known_rows_in_artist_is_verified,
    rows_in_artist_genre_moods_count_rank,
    last_moods_count_rank,
    ROUND(moods_count_rank / NULLIF(moods_count_max_2_prior, 0), 3) AS moods_count_rank_per_moods_count_max_2_prior,
    COALESCE(last_moods_count_rank, 0) AS last_moods_count_rank_filled
FROM sequenced
WHERE NOT artist_is_verified
ORDER BY artist_is_verified_sum_moods_count DESC
