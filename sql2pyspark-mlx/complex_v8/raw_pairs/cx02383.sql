WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
    WHERE LOWER(title) LIKE '%ng%'
      AND bpm < 166
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.release_date_week,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(release_date_week) IGNORE NULLS OVER (ORDER BY release_date_week ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_week,
        FIRST_VALUE(release_date_week) IGNORE NULLS OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_week,
        LEAD(release_date_week) OVER w_artist_is_verified_is_explic_2 AS next_release_date_week,
        NTILE(4) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week, id) AS release_date_week_quartile,
        ROW_NUMBER() OVER w_artist_is_verified_is_explic_2 AS release_date_week_seq,
        RANK() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week) AS release_date_week_rank,
        NTILE(10) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week, id) AS release_date_week_decile,
        ROW_NUMBER() OVER w_all_by_release_date_week AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week, id), 3) AS release_date_week_cume_dist,
        LAG(release_date_week, 2) OVER w_is_explicit_artist_is_verifi AS prev2_release_date_week,
        NTILE(10) OVER (ORDER BY release_date_week ASC, id) AS release_date_week_decile_2,
        RANK() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week, id) AS release_date_week_rank_2,
        NTH_VALUE(release_date_week, 2) OVER (ORDER BY release_date_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_week
    FROM enriched
    WINDOW w_artist_is_verified_is_explic_2 AS (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date_week, id), w_all_by_release_date_week AS (ORDER BY release_date_week, id), w_is_explicit_artist_is_verifi AS (PARTITION BY is_explicit, artist_is_verified ORDER BY release_date_week, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    moods,
    release_date_week,
    artist_is_verified,
    last_known_release_date_week,
    first_known_release_date_week,
    release_date_week_quartile,
    release_date_week_rank,
    seq_no,
    release_date_week_cume_dist,
    prev2_release_date_week,
    release_date_week_decile_2,
    release_date_week_rank_2,
    second_release_date_week,
    CONCAT(title, '#', CAST(release_date_week_rank_2 AS STRING)) AS title_ref
FROM windowed
ORDER BY first_known_release_date_week, second_release_date_week DESC
