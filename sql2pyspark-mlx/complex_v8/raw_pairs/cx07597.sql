WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d
    FROM plays
    WHERE ms_played >= 129336
      AND track_id <= 17
),
enriched AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_plus_1d,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
    WHERE LENGTH(TRIM(l.username)) >= 8
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY is_skipped)) AS distinct_track_id_in_is_skipped,
        NTILE(3) OVER (PARTITION BY source, TO_DATE(played_at_plus_1d) ORDER BY listener_birth_year, id) AS listener_birth_year_tercile,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY is_skipped), 2) AS listener_birth_year_vs_is_skipped_avg,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        SIZE(COLLECT_SET(device) OVER w_listener_id) AS distinct_device_in_listener_id,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        MAX(ms_played) OVER (PARTITION BY listener_id) AS listener_id_max_ms_played,
        MAX(ms_played) OVER (PARTITION BY source, TO_DATE(played_at_plus_1d) ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        SIZE(COLLECT_SET(track_id) OVER w_listener_id) AS distinct_track_id_in_listener_id
    FROM enriched
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        DATE_TRUNC('DAY', played_at_plus_1d) AS played_at_plus_1d_day
    FROM sequenced
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        ms_played,
        is_skipped,
        source,
        played_at_plus_1d,
        listener_username,
        listener_birth_year,
        distinct_track_id_in_is_skipped,
        listener_birth_year_tercile,
        listener_birth_year_vs_is_skipped_avg,
        rows_in_is_skipped,
        distinct_device_in_listener_id,
        rows_in_listener_id,
        listener_id_max_ms_played,
        max_ms_played_so_far,
        ms_played_bucket,
        played_at_plus_1d_day,
        SUM(listener_birth_year_tercile) OVER w_listener_id_distinct_track_i AS listener_id_distinct_track_id_in_is_skipped_sum_,
        ROUND(AVG(listener_id_max_ms_played) OVER (ORDER BY played_at_plus_1d_day NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS listener_id_max_ms_played_avg_3_prior,
        RANK() OVER (ORDER BY played_at_plus_1d_day NULLS LAST) AS played_at_plus_1d_day_rank,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER (PARTITION BY distinct_track_id_in_listener_id), 0), 4) AS ms_played_bucket_share_of_distinct_track_id_in_l,
        DENSE_RANK() OVER (ORDER BY played_at_plus_1d_day NULLS LAST) AS played_at_plus_1d_day_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY played_at_plus_1d_day NULLS LAST, id), 3) AS played_at_plus_1d_day_percent_rank,
        NTH_VALUE(listener_birth_year, 2) OVER (ORDER BY listener_birth_year_tercile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_birth_year,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY listener_id, distinct_track_id_in_is_skipped), 2) AS listener_birth_year_vs_listener_id_distinct_trac
    FROM scored
    WINDOW w_listener_id_distinct_track_i AS (PARTITION BY listener_id, distinct_track_id_in_is_skipped)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_skipped,
        distinct_track_id_in_is_skipped,
        played_at_plus_1d_day_rank,
        SUM(listener_birth_year_tercile) AS total_listener_birth_year_tercile,
        MAX(ms_played_bucket) AS max_ms_played_bucket
    FROM metrics
    GROUP BY is_skipped, distinct_track_id_in_is_skipped, played_at_plus_1d_day_rank
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        is_skipped,
        distinct_track_id_in_is_skipped,
        played_at_plus_1d_day_rank,
        total_listener_birth_year_tercile,
        max_ms_played_bucket,
        ROUND(played_at_plus_1d_day_rank - AVG(played_at_plus_1d_day_rank) OVER (PARTITION BY is_skipped, played_at_plus_1d_day_rank), 2) AS played_at_plus_1d_day_rank_vs_is_skipped_played_,
        ROW_NUMBER() OVER (ORDER BY total_listener_birth_year_tercile DESC, is_skipped, distinct_track_id_in_is_skipped, played_at_plus_1d_day_rank) AS rn,
        ROUND(PERCENT_RANK() OVER (ORDER BY total_listener_birth_year_tercile DESC, is_skipped, distinct_track_id_in_is_skipped, played_at_plus_1d_day_rank), 3) AS total_listener_birth_year_tercile_percent_rank
    FROM rollup_base
)
SELECT
    is_skipped,
    distinct_track_id_in_is_skipped,
    played_at_plus_1d_day_rank,
    total_listener_birth_year_tercile,
    max_ms_played_bucket,
    played_at_plus_1d_day_rank_vs_is_skipped_played_,
    rn,
    total_listener_birth_year_tercile_percent_rank
FROM metrics_2
WHERE rn BETWEEN 4 AND 6
