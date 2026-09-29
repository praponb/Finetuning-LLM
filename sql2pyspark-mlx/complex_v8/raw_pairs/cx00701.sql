WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE release_date >= DATE '2024-11-14'
      AND bpm >= 76
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS release_date_dense_rank,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_is_explicit, 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        LAG(duration_sec, 2) OVER w_artist_id_is_explicit_by_rel AS prev2_duration_sec,
        ROW_NUMBER() OVER w_is_explicit_by_release_date_2 AS release_date_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC), 3) AS duration_sec_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id), 3) AS release_date_cume_dist,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id_is_explicit, 0), 4) AS duration_sec_share_of_artist_id_is_explicit
    FROM base
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_id_is_explicit_by_rel AS (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id), w_is_explicit_by_release_date_2 AS (PARTITION BY is_explicit ORDER BY release_date DESC, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_seq) AS distinct_artist_id_in_release_date_seq,
        DENSE_RANK() OVER (ORDER BY first_known_release_date ASC, id) AS first_known_release_date_dense_rank,
        NTH_VALUE(duration_sec_percent_rank, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec_share_of_artist_id_is_explicit NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_percent_rank,
        ROUND(release_date_dense_rank - AVG(release_date_dense_rank) OVER w_release_date_seq, 2) AS release_date_dense_rank_vs_release_date_seq_avg
    FROM sequenced
    WINDOW w_release_date_seq AS (PARTITION BY release_date_seq)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    last_known_release_date,
    release_date_dense_rank,
    prev2_duration_sec,
    release_date_seq,
    release_date_cume_dist,
    first_known_release_date,
    duration_sec_share_of_artist_id_is_explicit,
    distinct_artist_id_in_release_date_seq,
    first_known_release_date_dense_rank
FROM ranked
ORDER BY first_known_release_date DESC, first_known_release_date_dense_rank DESC
