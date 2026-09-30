WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE track_id IS NOT NULL
      AND source <> 'search'
),
with_details AS (
    -- Attach listener attributes
    SELECT s.*, l.plan AS listener_plan, l.interests AS listener_interests
    FROM recent_plays s
    JOIN listeners l ON l.id = s.listener_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at NULLS LAST) AS played_at_dense_rank,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id_is_skipped, 2) AS ms_played_vs_track_id_is_skipped_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, is_skipped), 2) AS track_id_is_skipped_avg_ms_played,
        ROUND(source_len / NULLIF(SUM(source_len) OVER w_track_id_is_skipped, 0), 4) AS source_len_share_of_track_id_is_skipped,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_track_id_is_skipped, 0), 4) AS ms_played_share_of_track_id_is_skipped,
        DENSE_RANK() OVER w_all_by_source_len_ties AS source_len_dense_rank,
        LAG(played_at, 2) OVER (PARTITION BY is_skipped ORDER BY played_at NULLS LAST, id) AS prev2_played_at,
        ROUND(AVG(source_len) OVER (PARTITION BY is_skipped ORDER BY played_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS source_len_avg_7,
        SUM(source_len) OVER (ORDER BY source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_source_len
    FROM with_details
    WINDOW w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped), w_all_by_source_len_ties AS (ORDER BY source_len DESC)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        source_len,
        listener_plan,
        listener_interests,
        played_at_dense_rank,
        ms_played_vs_track_id_is_skipped_avg,
        track_id_is_skipped_avg_ms_played,
        source_len_share_of_track_id_is_skipped,
        ms_played_share_of_track_id_is_skipped,
        prev2_played_at,
        source_len_avg_7,
        running_source_len
    FROM sequenced
    WHERE source_len_dense_rank <= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY is_skipped ORDER BY source_len_share_of_track_id_is_skipped DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_max_4,
        ROUND(played_at_dense_rank / NULLIF(SUM(played_at_dense_rank) OVER (PARTITION BY listener_id), 0), 4) AS played_at_dense_rank_share_of_listener_id,
        ROUND(CUME_DIST() OVER w_all_by_played_at, 3) AS played_at_cume_dist,
        running_source_len - LAG(running_source_len) OVER (ORDER BY played_at ASC, id) AS running_source_len_change,
        NTILE(4) OVER (PARTITION BY is_skipped ORDER BY source_len_share_of_track_id_is_skipped DESC, id) AS source_len_share_of_track_id_is_skipped_quartile,
        ROUND(PERCENT_RANK() OVER w_is_skipped_by_source_len_sha_3, 3) AS source_len_share_of_track_id_is_skipped_percent_
    FROM first_rows
    WINDOW w_all_by_played_at AS (ORDER BY played_at ASC, id), w_is_skipped_by_source_len_sha_3 AS (PARTITION BY is_skipped ORDER BY source_len_share_of_track_id_is_skipped DESC)
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    is_skipped,
    source_len,
    listener_plan,
    listener_interests,
    played_at_dense_rank,
    track_id_is_skipped_avg_ms_played,
    source_len_share_of_track_id_is_skipped,
    ms_played_share_of_track_id_is_skipped,
    prev2_played_at,
    source_len_avg_7,
    ms_played_max_4,
    played_at_dense_rank_share_of_listener_id,
    played_at_cume_dist,
    running_source_len_change,
    source_len_share_of_track_id_is_skipped_quartile,
    source_len_share_of_track_id_is_skipped_percent_,
    DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
    CASE WHEN source_len_avg_7 >= 11.6 THEN 'A' WHEN source_len_avg_7 >= 4.4 THEN 'B' ELSE 'C' END AS source_len_avg_7_tier
FROM ranked
ORDER BY source_len_share_of_track_id_is_skipped_percent_ DESC NULLS FIRST
