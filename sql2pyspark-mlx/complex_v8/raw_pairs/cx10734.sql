WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket_2
    FROM plays
    WHERE listener_id >= 12
      AND ms_played < 218129
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at_2,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at_3
    FROM filtered_plays s
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id
    FROM summarized_rows s
    JOIN tracks t ON t.id = s.track_id
),
totals AS (
    -- Group-level totals
    SELECT
        device,
        DATE_TRUNC('MONTH', first_played_at) AS first_played_at_month,
        DATE_TRUNC('MONTH', first_played_at_3) AS first_played_at_3_month,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        MAX(first_played_at_3) AS last_first_played_at_3,
        MIN(first_played_at_3) AS first_first_played_at_3
    FROM with_details
    GROUP BY 1, 2, 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(first_played_at_3_month) OVER w_all_by_distinct_track_ids AS next_first_played_at_3_month,
        FIRST_VALUE(first_played_at_3_month) IGNORE NULLS OVER (PARTITION BY MONTH(last_first_played_at_3), device ORDER BY distinct_track_ids, device, first_played_at_month, first_played_at_3_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_played_at_3_month,
        NTH_VALUE(first_played_at_month, 2) OVER (PARTITION BY MONTH(first_played_at_3_month), YEAR(first_played_at_month) ORDER BY first_first_played_at_3 DESC, device, first_played_at_month, first_played_at_3_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_played_at_month,
        DENSE_RANK() OVER w_last_first_played_at_3_month_2 AS distinct_track_ids_dense_rank
    FROM totals
    WINDOW w_all_by_distinct_track_ids AS (ORDER BY distinct_track_ids DESC, device, first_played_at_month, first_played_at_3_month), w_last_first_played_at_3_month_2 AS (PARTITION BY MONTH(last_first_played_at_3), device ORDER BY distinct_track_ids)
),
derived AS (
    -- Add calculated columns
    SELECT
        device,
        first_played_at_month,
        first_played_at_3_month,
        first_first_played_at_3,
        second_first_played_at_month,
        distinct_track_ids_dense_rank,
        CASE WHEN device IN ('speaker', 'desktop') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group
    FROM windowed
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM derived
    WHERE distinct_track_ids_dense_rank = 1
)
-- Final projection
SELECT
    device,
    first_played_at_month,
    first_played_at_3_month,
    second_first_played_at_month,
    device_group,
    DATE_TRUNC('MONTH', first_first_played_at_3) AS first_first_played_at_3_month
FROM top_rows
ORDER BY first_played_at_month
