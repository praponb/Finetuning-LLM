WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN duration_sec >= 402 THEN 'large' WHEN duration_sec >= 164 THEN 'medium' ELSE 'small' END AS duration_sec_tier,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE bpm < 155
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, duration_sec_tier), 0), 4) AS bpm_share_of_is_explicit_duration_sec_tier,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY YEAR(release_date_year)), 0), 4) AS bpm_share_of_release_date_year_year,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit, duration_sec_tier), 2) AS duration_sec_vs_is_explicit_duration_sec_tier_av,
        COUNT(*) OVER (PARTITION BY YEAR(release_date_year)) AS rows_in_release_date_year_year,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY release_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        MAX(bpm) OVER (PARTITION BY artist_id, duration_sec_tier ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        MAX(bpm) OVER w_is_explicit_duration_sec_tie AS is_explicit_duration_sec_tier_max_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY release_date_year DESC), 3) AS release_date_year_cume_dist,
        SUM(duration_sec) OVER (ORDER BY release_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY bpm), 3) AS bpm_cume_dist,
        COUNT(*) OVER (PARTITION BY is_explicit, duration_sec_tier) AS rows_in_is_explicit_duration_sec_tier,
        MAX(bpm) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_max_1_prior,
        DENSE_RANK() OVER (ORDER BY release_date_year DESC) AS release_date_year_dense_rank,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_2
    FROM recent
    WINDOW w_is_explicit_duration_sec_tie AS (PARTITION BY is_explicit, duration_sec_tier)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_tier,
        release_date_year,
        bpm_share_of_is_explicit_duration_sec_tier,
        bpm_share_of_release_date_year_year,
        duration_sec_vs_is_explicit_duration_sec_tier_av,
        rows_in_release_date_year_year,
        last_known_bpm,
        max_bpm_so_far,
        is_explicit_duration_sec_tier_max_bpm,
        release_date_year_cume_dist,
        running_duration_sec,
        bpm_cume_dist,
        rows_in_is_explicit_duration_sec_tier,
        bpm_max_1_prior,
        last_known_bpm_2
    FROM with_windows
    WHERE release_date_year_dense_rank = 1
      AND duration_sec_vs_is_explicit_duration_sec_tier_av < 294.0
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(last_known_bpm) OVER (PARTITION BY duration_sec_tier ORDER BY duration_sec_vs_is_explicit_duration_sec_tier_av ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS last_known_bpm_avg_2,
        NTH_VALUE(last_known_bpm_2, 2) OVER (ORDER BY release_date_year_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_bpm_2,
        NTH_VALUE(last_known_bpm_2, 2) OVER (PARTITION BY duration_sec_tier ORDER BY duration_sec_vs_is_explicit_duration_sec_tier_av ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_bpm_2_2,
        NTILE(5) OVER w_duration_sec_tier_by_duratio_3 AS duration_sec_vs_is_explicit_duration_sec_tier_av_2,
        NTH_VALUE(last_known_bpm_2, 2) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date_year) ORDER BY release_date_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_bpm_2_3,
        SUM(max_bpm_so_far) OVER (ORDER BY release_date_year_cume_dist, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS max_bpm_so_far_sum_4,
        ROUND(release_date_year_cume_dist - AVG(release_date_year_cume_dist) OVER (PARTITION BY duration_sec_tier, artist_id), 2) AS release_date_year_cume_dist_vs_duration_sec_tier,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY duration_sec_tier)) AS distinct_artist_id_in_duration_sec_tier
    FROM top_per_group
    WINDOW w_duration_sec_tier_by_duratio_3 AS (PARTITION BY duration_sec_tier ORDER BY duration_sec_vs_is_explicit_duration_sec_tier_av ASC, id)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm,
    moods,
    release_date_year,
    bpm_share_of_is_explicit_duration_sec_tier,
    bpm_share_of_release_date_year_year,
    duration_sec_vs_is_explicit_duration_sec_tier_av,
    rows_in_release_date_year_year,
    last_known_bpm,
    max_bpm_so_far,
    release_date_year_cume_dist,
    running_duration_sec,
    bpm_cume_dist,
    rows_in_is_explicit_duration_sec_tier,
    bpm_max_1_prior,
    last_known_bpm_2,
    last_known_bpm_avg_2,
    second_last_known_bpm_2,
    second_last_known_bpm_2_2,
    second_last_known_bpm_2_3,
    release_date_year_cume_dist_vs_duration_sec_tier,
    distinct_artist_id_in_duration_sec_tier,
    DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
    GREATEST(last_known_bpm_2, max_bpm_so_far) AS greatest_last_known_bpm_2_max_bpm_so_far
FROM windowed
WHERE max_bpm_so_far <= 1191
ORDER BY last_known_bpm_avg_2, max_bpm_so_far
