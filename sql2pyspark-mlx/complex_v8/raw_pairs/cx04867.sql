WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE device IS NOT NULL
      AND track_id >= 23
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY played_at DESC, id) AS played_at_seq,
        LAST_VALUE(played_at) OVER (PARTITION BY track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        LEAD(played_at, 2) OVER w_track_id_listener_id_by_play AS next2_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(PERCENT_RANK() OVER w_track_id_listener_id_by_play_3, 3) AS played_at_percent_rank
    FROM valid_plays
    WINDOW w_track_id_listener_id_by_play AS (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id), w_track_id_listener_id_by_play_3 AS (PARTITION BY track_id, listener_id ORDER BY played_at DESC)
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sequenced s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.ms_played >= 151112
              AND p.track_id IS NOT NULL
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id, YEAR(second_played_at))) AS distinct_listener_id_in_track_id_second_played_a,
        ROUND(played_at_seq - AVG(played_at_seq) OVER (PARTITION BY track_id, YEAR(second_played_at)), 2) AS played_at_seq_vs_track_id_second_played_at_year_,
        ROUND(played_at_percent_rank / NULLIF(SUM(played_at_percent_rank) OVER (PARTITION BY track_id, YEAR(second_played_at)), 0), 4) AS played_at_percent_rank_share_of_track_id_second_,
        LAST_VALUE(next2_played_at) IGNORE NULLS OVER (ORDER BY minutes_since_prev_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next2_played_at,
        ROUND((UNIX_TIMESTAMP(last_played_at) - UNIX_TIMESTAMP(LAG(last_played_at) OVER (ORDER BY second_played_at ASC, id))) / 60, 1) AS minutes_since_prev_last_played_at
    FROM with_activity
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device NOT IN ('phone', 'tv')
              AND p.ms_played <= 241585
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(device) OVER w_track_id) AS distinct_device_in_track_id,
        SUM(played_at_seq) OVER (PARTITION BY track_id) AS track_id_sum_played_at_seq,
        MAX(played_at_percent_rank) OVER (PARTITION BY YEAR(second_played_at), source ORDER BY last_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_played_at_percent_rank_so_far,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id
    FROM qualified
    WINDOW w_track_id AS (PARTITION BY track_id)
)
SELECT *
FROM (
    SELECT source, played_at_seq, device, distinct_listener_id_in_track_id_second_played_a
    FROM metrics
) src
PIVOT (
    COUNT(distinct_listener_id_in_track_id_second_played_a)
    FOR device IN ('car', 'phone', 'desktop')
)
