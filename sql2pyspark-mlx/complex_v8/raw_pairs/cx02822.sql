WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE duration_sec BETWEEN 144 AND 352
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        s.greatest_duration_sec_bpm,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM recent s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(greatest_duration_sec_bpm) IGNORE NULLS OVER (PARTITION BY artist_genre, artist_id ORDER BY greatest_duration_sec_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_greatest_duration_sec_bpm,
        NTILE(5) OVER w_artist_genre_artist_id_by_gr_2 AS greatest_duration_sec_bpm_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_genre ORDER BY greatest_duration_sec_bpm ASC NULLS LAST), 3) AS greatest_duration_sec_bpm_cume_dist,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, artist_country), 2) AS duration_sec_vs_artist_id_artist_country_avg,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY artist_id, artist_country)) AS distinct_artist_genre_in_artist_id_artist_countr,
        LAG(greatest_duration_sec_bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS prev_greatest_duration_sec_bpm,
        FIRST_VALUE(greatest_duration_sec_bpm) OVER (PARTITION BY artist_genre, artist_id ORDER BY greatest_duration_sec_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_greatest_duration_sec_bpm,
        MIN(duration_sec) OVER (PARTITION BY artist_country, is_explicit) AS artist_country_is_explicit_min_duration_sec,
        RANK() OVER (PARTITION BY artist_genre, artist_is_verified ORDER BY duration_sec DESC) AS duration_sec_rank,
        ROUND(greatest_duration_sec_bpm / NULLIF(SUM(greatest_duration_sec_bpm) OVER (PARTITION BY artist_country, is_explicit), 0), 4) AS greatest_duration_sec_bpm_share_of_artist_countr,
        SIZE(COLLECT_SET(artist_genre) OVER w_artist_country_is_explicit) AS distinct_artist_genre_in_artist_country_is_expli
    FROM joined_artist
    WINDOW w_artist_genre_artist_id_by_gr_2 AS (PARTITION BY artist_genre, artist_id ORDER BY greatest_duration_sec_bpm DESC, id), w_artist_country_is_explicit AS (PARTITION BY artist_country, is_explicit)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(greatest_duration_sec_bpm / NULLIF(SUM(greatest_duration_sec_bpm) OVER w_artist_country, 0), 4) AS greatest_duration_sec_bpm_share_of_artist_countr_2,
        MIN(greatest_duration_sec_bpm) OVER (PARTITION BY artist_country) AS artist_country_min_greatest_duration_sec_bpm,
        LEAD(greatest_duration_sec_bpm) OVER (PARTITION BY artist_genre, artist_is_verified ORDER BY distinct_artist_genre_in_artist_country_is_expli DESC, id) AS next_greatest_duration_sec_bpm,
        NTH_VALUE(duration_sec_rank, 2) OVER (PARTITION BY artist_is_verified, artist_country ORDER BY prev_greatest_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_rank,
        RANK() OVER (PARTITION BY distinct_artist_genre_in_artist_country_is_expli ORDER BY artist_country_is_explicit_min_duration_sec ASC, id) AS artist_country_is_explicit_min_duration_sec_rank
    FROM with_trends
    WINDOW w_artist_country AS (PARTITION BY artist_country)
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    moods,
    greatest_duration_sec_bpm,
    artist_genre,
    artist_country,
    artist_is_verified,
    last_known_greatest_duration_sec_bpm,
    greatest_duration_sec_bpm_quintile,
    distinct_artist_genre_in_artist_id_artist_countr,
    prev_greatest_duration_sec_bpm,
    artist_country_is_explicit_min_duration_sec,
    duration_sec_rank,
    greatest_duration_sec_bpm_share_of_artist_countr,
    distinct_artist_genre_in_artist_country_is_expli,
    greatest_duration_sec_bpm_share_of_artist_countr_2,
    artist_country_min_greatest_duration_sec_bpm,
    next_greatest_duration_sec_bpm,
    second_duration_sec_rank,
    artist_country_is_explicit_min_duration_sec_rank
FROM windowed
WHERE duration_sec_vs_artist_id_artist_country_avg BETWEEN -252.0 AND 378.0
ORDER BY artist_id, distinct_artist_genre_in_artist_id_artist_countr
