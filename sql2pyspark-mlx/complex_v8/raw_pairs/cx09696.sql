WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        CASE WHEN duration_sec >= 353 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END AS duration_sec_tier,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
    WHERE NOT is_explicit
      AND bpm <= 142
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_release_date_plus_7d, 3) AS release_date_plus_7d_cume_dist,
        LAST_VALUE(release_date_plus_7d) OVER (ORDER BY release_date_plus_7d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_plus_7d,
        LAST_VALUE(release_date_plus_7d) OVER (PARTITION BY artist_id ORDER BY release_date_plus_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_plus_7d_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date_plus_7d, id), 3) AS release_date_plus_7d_percent_rank,
        LAST_VALUE(release_date_plus_7d) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date_plus_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date_plus_7d,
        FIRST_VALUE(release_date_plus_7d) OVER (ORDER BY release_date_plus_7d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_plus_7d,
        ROW_NUMBER() OVER (ORDER BY release_date_plus_7d DESC, id) AS seq_no,
        LEAD(release_date_plus_7d, 3) OVER (ORDER BY release_date_plus_7d DESC, id) AS next3_release_date_plus_7d,
        NTILE(10) OVER w_is_explicit_by_release_date AS release_date_plus_7d_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY release_date_plus_7d), 3) AS release_date_plus_7d_cume_dist_3
    FROM filtered
    WINDOW w_all_by_release_date_plus_7d AS (ORDER BY release_date_plus_7d DESC, id), w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date_plus_7d, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE seq_no <= 3
)
-- Final projection
SELECT
    id,
    title,
    is_explicit,
    moods,
    duration_sec_tier,
    release_date_plus_7d,
    release_date_plus_7d_cume_dist,
    last_release_date_plus_7d_2,
    last_known_release_date_plus_7d,
    first_release_date_plus_7d,
    next3_release_date_plus_7d,
    release_date_plus_7d_decile,
    release_date_plus_7d_cume_dist_3,
    ADD_MONTHS(release_date_plus_7d, 1) AS release_date_plus_7d_plus_1m,
    LOWER(REGEXP_REPLACE(TRIM(duration_sec_tier), ' ', '_')) AS duration_sec_tier_slug
FROM top_per_group
