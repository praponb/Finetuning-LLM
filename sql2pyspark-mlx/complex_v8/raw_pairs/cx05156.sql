WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter
    FROM plays
    WHERE HOUR(played_at) BETWEEN 6 AND 10
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        l.interests AS listener_interests,
        l.plan AS listener_plan
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(played_at) OVER (PARTITION BY track_id ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at DESC NULLS FIRST), 3) AS played_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY played_at NULLS LAST, id) AS rn,
        NTILE(10) OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id) AS played_at_decile,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        DENSE_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at DESC) AS played_at_dense_rank,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at DESC), 3) AS played_at_percent_rank_2,
        LAST_VALUE(played_at) OVER (ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_2,
        LAST_VALUE(played_at) OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_2,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id), 3) AS played_at_cume_dist
    FROM with_parent
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(played_at_cume_dist) OVER (ORDER BY first_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY listener_id, is_skipped ORDER BY last_played_at_2 DESC, id) AS seq_no,
        SUM(played_at_percent_rank_2) OVER (PARTITION BY track_id) AS track_id_sum_played_at_percent_rank_2,
        ROUND(played_at_decile / NULLIF(SUM(played_at_decile) OVER (PARTITION BY track_id), 0), 4) AS played_at_decile_share_of_track_id,
        SUM(played_at_dense_rank) OVER w_track_id AS track_id_sum_played_at_dense_rank,
        SUM(rn) OVER (PARTITION BY listener_id, is_skipped ORDER BY last_played_at_2 DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rn_sum_7,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id
    FROM sequenced
    WINDOW w_track_id AS (PARTITION BY track_id)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    listener_interests,
    listener_plan,
    first_played_at,
    played_at_percent_rank,
    played_at_decile,
    second_played_at,
    first_known_played_at,
    played_at_percent_rank_2,
    last_played_at,
    first_known_played_at_2,
    last_played_at_2,
    played_at_cume_dist,
    first_played_at_cume_dist,
    seq_no,
    track_id_sum_played_at_percent_rank_2,
    played_at_decile_share_of_track_id,
    track_id_sum_played_at_dense_rank,
    rn_sum_7,
    rows_in_track_id,
    ROUND(played_at_percent_rank * 1.07, 2) AS played_at_percent_rank_adj
FROM sequenced_2
ORDER BY listener_plan DESC
