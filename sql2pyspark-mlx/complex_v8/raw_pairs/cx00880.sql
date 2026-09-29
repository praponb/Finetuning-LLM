WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE bpm <= 142
      AND duration_sec >= 164
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, a.country AS artist_country
    FROM cleaned s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.is_verified = FALSE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(bpm) OVER (PARTITION BY artist_country ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior,
        NTILE(3) OVER (PARTITION BY artist_country ORDER BY bpm DESC, id) AS bpm_tercile,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        SUM(bpm) OVER (PARTITION BY artist_country ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_id)) AS distinct_artist_country_in_artist_id,
        NTH_VALUE(bpm, 2) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        NTILE(10) OVER w_all_by_release_date AS release_date_decile
    FROM combined_details
    WINDOW w_all_by_release_date AS (ORDER BY release_date, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        bpm,
        moods,
        bpm_sum_6_prior,
        bpm_tercile,
        bpm_vs_artist_id_avg,
        running_bpm,
        distinct_artist_country_in_artist_id,
        SUBSTRING(TRIM(moods_text), 1, 3) AS moods_text_prefix
    FROM with_trends
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(bpm_vs_artist_id_avg / NULLIF(SUM(bpm_vs_artist_id_avg) OVER (PARTITION BY bpm_tercile), 0), 4) AS bpm_vs_artist_id_avg_share_of_bpm_tercile,
        ROUND(AVG(bpm_sum_6_prior) OVER (PARTITION BY distinct_artist_country_in_artist_id ORDER BY bpm_tercile, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS bpm_sum_6_prior_avg_4,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm_sum_6_prior DESC), 3) AS bpm_sum_6_prior_percent_rank,
        LAG(bpm_vs_artist_id_avg) OVER (PARTITION BY distinct_artist_country_in_artist_id ORDER BY bpm_tercile, id) AS prev_bpm_vs_artist_id_avg,
        LAG(bpm, 3) OVER (ORDER BY bpm_sum_6_prior DESC, id) AS prev3_bpm,
        COUNT(*) OVER w_bpm_tercile AS rows_in_bpm_tercile,
        FIRST_VALUE(bpm_tercile) OVER (ORDER BY bpm_sum_6_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_tercile
    FROM tagged
    WINDOW w_bpm_tercile AS (PARTITION BY bpm_tercile)
)
-- Final projection
SELECT
    id,
    bpm_sum_6_prior,
    bpm_vs_artist_id_avg,
    running_bpm,
    distinct_artist_country_in_artist_id,
    moods_text_prefix,
    bpm_vs_artist_id_avg_share_of_bpm_tercile,
    bpm_sum_6_prior_avg_4,
    bpm_sum_6_prior_percent_rank,
    prev_bpm_vs_artist_id_avg,
    prev3_bpm,
    rows_in_bpm_tercile,
    first_bpm_tercile
FROM sequenced
WHERE distinct_artist_country_in_artist_id > 4
ORDER BY bpm_vs_artist_id_avg DESC
