WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        CASE WHEN duration_sec >= 357 THEN 'large' WHEN duration_sec >= 184 THEN 'medium' ELSE 'small' END AS duration_sec_tier,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE SIZE(moods) >= 2
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device IN ('car', 'phone', 'speaker')
        )
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.moods,
        s.duration_sec_tier,
        s.duration_sec_filled,
        a.artist_name
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.genre IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(duration_sec_tier) OVER (PARTITION BY artist_id)) AS distinct_duration_sec_tier_in_artist_id,
        MAX(duration_sec_filled) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec_filled,
        FIRST_VALUE(duration_sec_filled) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_filled,
        LEAD(duration_sec_filled) OVER (ORDER BY release_date, id) AS next_duration_sec_filled,
        ROUND(duration_sec_filled - AVG(duration_sec_filled) OVER (PARTITION BY artist_id), 2) AS duration_sec_filled_vs_artist_id_avg,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id
    FROM with_parent_artist
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(first_duration_sec_filled) OVER (PARTITION BY duration_sec_tier, artist_id) AS duration_sec_tier_artist_id_sum_first_duration_s,
        MAX(rows_in_artist_id) OVER (PARTITION BY duration_sec_tier, artist_id) AS duration_sec_tier_artist_id_max_rows_in_artist_i,
        LEAD(duration_sec_filled, 2) OVER w_release_date_month_duration AS next2_duration_sec_filled,
        NTH_VALUE(duration_sec_filled, 2) OVER (PARTITION BY duration_sec_tier, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_filled,
        ROUND(first_duration_sec_filled - AVG(first_duration_sec_filled) OVER (PARTITION BY duration_sec_tier, artist_id), 2) AS first_duration_sec_filled_vs_duration_sec_tier_a
    FROM ranked
    WINDOW w_release_date_month_duration AS (PARTITION BY DATE_TRUNC('MONTH', release_date), duration_sec_tier ORDER BY duration_sec_filled, id)
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.duration_sec_tier,
        s.duration_sec_filled,
        s.artist_name,
        s.distinct_duration_sec_tier_in_artist_id,
        s.artist_id_max_duration_sec_filled,
        s.first_duration_sec_filled,
        s.next_duration_sec_filled,
        s.duration_sec_filled_vs_artist_id_avg,
        s.rows_in_artist_id,
        s.duration_sec_tier_artist_id_sum_first_duration_s,
        s.duration_sec_tier_artist_id_max_rows_in_artist_i,
        s.next2_duration_sec_filled,
        s.second_duration_sec_filled,
        s.first_duration_sec_filled_vs_duration_sec_tier_a,
        t.mood
    FROM with_windows s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Final projection
SELECT
    id,
    artist_id,
    artist_name,
    distinct_duration_sec_tier_in_artist_id,
    first_duration_sec_filled,
    next_duration_sec_filled,
    duration_sec_filled_vs_artist_id_avg,
    rows_in_artist_id,
    duration_sec_tier_artist_id_sum_first_duration_s,
    next2_duration_sec_filled,
    first_duration_sec_filled_vs_duration_sec_tier_a,
    mood,
    MONTH(release_date) AS release_date_month,
    SUBSTRING(TRIM(duration_sec_tier), 1, 2) AS duration_sec_tier_prefix
FROM flattened
ORDER BY duration_sec_filled_vs_artist_id_avg DESC
