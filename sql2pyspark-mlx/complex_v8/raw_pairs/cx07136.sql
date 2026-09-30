WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        HOUR(played_at) AS played_at_hour,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM plays
    WHERE (source IN ('radio', 'album') OR ms_played > 151112)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        is_skipped_label,
        MAX(played_at_hour) OVER (PARTITION BY played_at_hour, is_skipped) AS played_at_hour_is_skipped_max_played_at_hour,
        ROUND(played_at_hour / NULLIF(SUM(played_at_hour) OVER (PARTITION BY played_at_hour, is_skipped), 0), 4) AS played_at_hour_share_of_played_at_hour_is_skippe,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_hour_ties, 3) AS played_at_hour_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at), 3) AS played_at_percent_rank,
        LAG(played_at, 3) OVER (ORDER BY played_at, id) AS prev3_played_at,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at_hour NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at
    FROM valid_plays
    WINDOW w_all_by_played_at_hour_ties AS (ORDER BY played_at_hour NULLS LAST)
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.is_skipped_label,
        s.played_at_hour_is_skipped_max_played_at_hour,
        s.played_at_hour_share_of_played_at_hour_is_skippe,
        s.played_at_percent_rank,
        s.prev3_played_at,
        s.first_known_played_at,
        t.moods AS track_moods,
        t.title AS track_title
    FROM with_trends s
    JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(played_at_percent_rank) OVER (ORDER BY prev3_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_percent_rank,
        ROUND(AVG(played_at_percent_rank) OVER (ORDER BY prev3_played_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS played_at_percent_rank_avg_1_prior,
        COUNT(*) OVER w_source_is_skipped_label AS rows_in_source_is_skipped_label,
        SUM(played_at_percent_rank) OVER (ORDER BY prev3_played_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS played_at_percent_rank_sum_3_prior,
        LAST_VALUE(first_known_played_at) OVER (ORDER BY prev3_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_known_played_at,
        MAX(played_at_percent_rank) OVER (ORDER BY played_at_hour_share_of_played_at_hour_is_skippe DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS played_at_percent_rank_max_6_prior
    FROM enriched
    WINDOW w_source_is_skipped_label AS (PARTITION BY source, is_skipped_label)
),
latest_per_key AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        is_skipped_label,
        played_at_hour_is_skipped_max_played_at_hour,
        played_at_hour_share_of_played_at_hour_is_skippe,
        played_at_percent_rank,
        prev3_played_at,
        first_known_played_at,
        track_moods,
        track_title,
        last_played_at_percent_rank,
        played_at_percent_rank_avg_1_prior,
        rows_in_source_is_skipped_label,
        played_at_percent_rank_sum_3_prior,
        last_first_known_played_at,
        played_at_percent_rank_max_6_prior
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at_percent_rank_max_6_prior DESC, id DESC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.played_at_hour_is_skipped_max_played_at_hour,
        s.prev3_played_at,
        s.first_known_played_at,
        s.track_moods,
        s.track_title,
        s.played_at_percent_rank_avg_1_prior,
        s.rows_in_source_is_skipped_label,
        s.played_at_percent_rank_sum_3_prior,
        s.played_at_percent_rank_max_6_prior,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date,
        t.moods AS track_moods_2
    FROM latest_per_key s
    JOIN tracks t ON t.id = s.track_id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM enriched_track s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND TO_DATE(p.played_at) < DATE '2025-03-08'
              AND p.device = 'tv'
        )
      AND s.played_at_percent_rank_sum_3_prior > 0.3
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    first_known_played_at,
    track_moods,
    track_title,
    played_at_percent_rank_avg_1_prior,
    rows_in_source_is_skipped_label,
    track_release_date
FROM filtered
ORDER BY track_id NULLS LAST, id
LIMIT 20
