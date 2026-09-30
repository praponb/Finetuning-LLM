WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'N%'
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.is_explicit_flag,
        a.artist_name,
        a.genre AS artist_genre
    FROM cleaned_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        is_explicit_flag - LAG(is_explicit_flag) OVER w_artist_genre_by_duration_sec AS is_explicit_flag_change,
        SIZE(COLLECT_SET(artist_genre) OVER (PARTITION BY is_explicit_flag)) AS distinct_artist_genre_in_is_explicit_flag,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER (PARTITION BY is_explicit_flag), 0), 4) AS is_explicit_flag_share_of_is_explicit_flag,
        COUNT(*) OVER (PARTITION BY is_explicit_flag) AS rows_in_is_explicit_flag,
        DENSE_RANK() OVER (PARTITION BY artist_genre ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit_flag), 0), 4) AS duration_sec_share_of_is_explicit_flag,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_genre ORDER BY duration_sec DESC NULLS FIRST, id), 3) AS duration_sec_cume_dist,
        LAG(duration_sec) OVER w_artist_genre_by_duration_sec AS prev_duration_sec,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_genre ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_4
    FROM combined_details_artist
    WINDOW w_artist_genre_by_duration_sec AS (PARTITION BY artist_genre ORDER BY duration_sec DESC NULLS FIRST, id), w_is_explicit_flag AS (PARTITION BY is_explicit_flag)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        duration_sec_dense_rank,
        CASE WHEN prev_duration_sec >= 312 THEN 'large' WHEN prev_duration_sec >= 209 THEN 'medium' ELSE 'small' END AS prev_duration_sec_tier,
        distinct_artist_genre_in_is_explicit_flag,
        ROUND(SUM(duration_sec_dense_rank) / NULLIF(SUM(duration_sec_cume_dist), 0), 3) AS duration_sec_dense_rank_to_duration_sec_cume_dis,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM sequenced
    WHERE duration_sec_share_of_is_explicit_flag > 0.2
    GROUP BY duration_sec_dense_rank, CASE WHEN prev_duration_sec >= 312 THEN 'large' WHEN prev_duration_sec >= 209 THEN 'medium' ELSE 'small' END, distinct_artist_genre_in_is_explicit_flag
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(distinct_artist_ids) IGNORE NULLS OVER (PARTITION BY duration_sec_dense_rank ORDER BY distinct_artist_ids, duration_sec_dense_rank, prev_duration_sec_tier, distinct_artist_genre_in_is_explicit_flag ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_artist_ids,
        MAX(duration_sec_dense_rank) OVER (ORDER BY distinct_artist_ids DESC, duration_sec_dense_rank, prev_duration_sec_tier, distinct_artist_genre_in_is_explicit_flag ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_dense_rank_max_1_prior,
        ROUND(distinct_artist_ids - AVG(distinct_artist_ids) OVER w_prev_duration_sec_tier_durat, 2) AS distinct_artist_ids_vs_prev_duration_sec_tier_du,
        MAX(duration_sec_dense_rank) OVER (PARTITION BY prev_duration_sec_tier, duration_sec_dense_rank) AS prev_duration_sec_tier_duration_sec_dense_rank_m
    FROM totals
    WINDOW w_prev_duration_sec_tier_durat AS (PARTITION BY prev_duration_sec_tier, duration_sec_dense_rank)
)
-- Final projection
SELECT
    duration_sec_dense_rank,
    distinct_artist_genre_in_is_explicit_flag,
    last_known_distinct_artist_ids,
    duration_sec_dense_rank_max_1_prior,
    distinct_artist_ids_vs_prev_duration_sec_tier_du,
    prev_duration_sec_tier_duration_sec_dense_rank_m,
    GREATEST(distinct_artist_ids, duration_sec_dense_rank) AS greatest_distinct_artist_ids_duration_sec_dense_,
    LOWER(REGEXP_REPLACE(TRIM(prev_duration_sec_tier), ' ', '_')) AS prev_duration_sec_tier_slug
FROM with_windows
WHERE duration_sec_dense_rank_max_1_prior < 20
ORDER BY prev_duration_sec_tier_slug
