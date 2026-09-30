WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 220498 THEN 'A' WHEN ms_played >= 92277 THEN 'B' ELSE 'C' END AS ms_played_tier,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE device IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        played_at_label,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY source, listener_id ORDER BY played_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_id ORDER BY played_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at
    FROM cleaned
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', second_played_at) AS second_played_at_month,
        listener_id,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS n_is_skipped_true
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', second_played_at), listener_id
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.second_played_at_month,
        s.listener_id,
        s.distinct_listener_ids,
        s.n_is_skipped_true,
        l.interests AS listener_interests,
        l.username AS listener_username
    FROM grouped s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.username IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        second_played_at_month,
        listener_id,
        distinct_listener_ids,
        n_is_skipped_true,
        listener_username,
        SUM(distinct_listener_ids) OVER (ORDER BY n_is_skipped_true, second_played_at_month, listener_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS distinct_listener_ids_sum_3_prior,
        DENSE_RANK() OVER (ORDER BY n_is_skipped_true) AS n_is_skipped_true_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(second_played_at_month), n_is_skipped_true ORDER BY distinct_listener_ids DESC), 3) AS distinct_listener_ids_percent_rank,
        FIRST_VALUE(distinct_listener_ids) IGNORE NULLS OVER (ORDER BY n_is_skipped_true, second_played_at_month, listener_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_listener_ids
    FROM combined_details
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        HOUR(second_played_at_month) AS second_played_at_month_hour,
        ROUND(distinct_listener_ids / NULLIF(n_is_skipped_true, 0), 3) AS distinct_listener_ids_per_n_is_skipped_true,
        ROUND(n_is_skipped_true / NULLIF(distinct_listener_ids, 0), 3) AS n_is_skipped_true_per_distinct_listener_ids
    FROM with_windows
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.second_played_at_month,
        s.listener_id,
        s.distinct_listener_ids,
        s.n_is_skipped_true,
        s.listener_username,
        s.n_is_skipped_true_dense_rank,
        s.first_known_distinct_listener_ids,
        s.second_played_at_month_hour,
        s.distinct_listener_ids_per_n_is_skipped_true,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.track_id AS play_track_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM labeled s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    second_played_at_month,
    distinct_listener_ids,
    n_is_skipped_true_dense_rank,
    play_id,
    play_is_skipped,
    play_track_id,
    play_ms_played,
    play_source
FROM exploded_detail_plays
WHERE n_is_skipped_true_dense_rank <= 7
ORDER BY play_track_id NULLS LAST
