WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm >= 76
      AND ARRAY_CONTAINS(moods, 'party')
),
with_details AS (
    -- Join artists
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LAG(artist_debut_year) OVER (ORDER BY duration_sec ASC, id) AS prev_artist_debut_year,
        RANK() OVER (ORDER BY duration_sec ASC) AS duration_sec_rank,
        LAST_VALUE(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        duration_sec - LAG(duration_sec) OVER (ORDER BY bpm DESC, id) AS duration_sec_change,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        NTH_VALUE(artist_debut_year, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        LAST_VALUE(artist_debut_year) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year_2,
        LAG(bpm, 3) OVER (ORDER BY duration_sec DESC, id) AS prev3_bpm,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM with_details
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        is_explicit,
        moods,
        prev_artist_debut_year,
        duration_sec_change,
        distinct_artist_id_in_is_explicit,
        second_artist_debut_year,
        last_artist_debut_year_2,
        MAX(prev3_bpm) OVER (PARTITION BY duration_sec_rank ORDER BY duration_sec_change, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev3_bpm_max_3,
        ROUND(AVG(bpm) OVER (PARTITION BY duration_sec_rank, distinct_artist_id_in_is_explicit ORDER BY bpm, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_2_prior,
        ROUND(CUME_DIST() OVER w_duration_sec_rank_distinct_a_2, 3) AS bpm_cume_dist,
        COUNT(*) OVER (PARTITION BY duration_sec_rank, is_explicit) AS rows_in_duration_sec_rank_is_explicit,
        SUM(duration_sec_rank) OVER w_duration_sec_rank_is_explici AS duration_sec_rank_is_explicit_sum_duration_sec_r,
        LEAD(last_artist_debut_year_2) OVER w_duration_sec_rank_distinct_a_3 AS next_last_artist_debut_year_2,
        SIZE(COLLECT_SET(artist_id) OVER w_duration_sec_rank_is_explici) AS distinct_artist_id_in_duration_sec_rank_is_expli,
        ROUND(CUME_DIST() OVER (ORDER BY second_artist_debut_year NULLS LAST), 3) AS second_artist_debut_year_cume_dist
    FROM metrics
    WINDOW w_duration_sec_rank_distinct_a_2 AS (PARTITION BY duration_sec_rank, distinct_artist_id_in_is_explicit ORDER BY bpm), w_duration_sec_rank_is_explici AS (PARTITION BY duration_sec_rank, is_explicit), w_duration_sec_rank_distinct_a_3 AS (PARTITION BY duration_sec_rank, distinct_artist_id_in_is_explicit ORDER BY bpm, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    moods,
    prev_artist_debut_year,
    duration_sec_change,
    distinct_artist_id_in_is_explicit,
    second_artist_debut_year,
    last_artist_debut_year_2,
    prev3_bpm_max_3,
    bpm_avg_2_prior,
    bpm_cume_dist,
    distinct_artist_id_in_duration_sec_rank_is_expli,
    second_artist_debut_year_cume_dist,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label
FROM sequenced
ORDER BY id DESC, duration_sec_change DESC NULLS FIRST
