WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods
    FROM tracks
    WHERE duration_sec < 402
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC), 3) AS duration_sec_percent_rank,
        MAX(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_max_1_prior,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_6_prior,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        ROUND(AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        MAX(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_max_7,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_percent_rank_2
    FROM valid_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(duration_sec_avg_2 * 1.07, 2) AS duration_sec_avg_2_adj,
        ABS(duration_sec_max_7 - 558) AS duration_sec_max_7_distance
    FROM with_trends
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        duration_sec_max_1_prior,
        duration_sec_vs_artist_id_avg,
        artist_id_max_duration_sec,
        duration_sec_sum_6_prior,
        duration_sec_share_of_artist_id,
        duration_sec_avg_2,
        duration_sec_percent_rank_2,
        duration_sec_avg_2_adj,
        duration_sec_max_7_distance,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY first_known_duration_sec), 3) AS first_known_duration_sec_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY first_known_duration_sec, id) AS rn,
        NTILE(5) OVER (PARTITION BY artist_id ORDER BY duration_sec_avg_2_adj ASC, id) AS duration_sec_avg_2_adj_quintile,
        COUNT(*) OVER (ORDER BY artist_id_max_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (PARTITION BY artist_id ORDER BY first_known_duration_sec, id) AS first_known_duration_sec_decile,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY artist_id_max_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        LEAD(duration_sec_avg_2_adj, 1, 0) OVER w_artist_id_by_duration_sec_av AS next_duration_sec_avg_2_adj
    FROM labeled
    WINDOW w_artist_id_by_duration_sec_av AS (PARTITION BY artist_id ORDER BY duration_sec_avg_2_adj ASC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        rn,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked
    GROUP BY 1
)
-- Final projection
SELECT
    rn,
    title_list,
    distinct_artist_ids,
    distinct_ids,
    CASE WHEN rn >= 7 THEN 'A' WHEN rn >= 4 THEN 'B' ELSE 'C' END AS rn_tier
FROM grouped
WHERE distinct_ids <= 7
ORDER BY rn_tier, distinct_ids DESC NULLS FIRST
