WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY source) AS source_max_ms_played,
        DENSE_RANK() OVER (PARTITION BY track_id, source ORDER BY played_at, id) AS played_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY track_id, source ORDER BY played_at, id) AS rn,
        SUM(ms_played) OVER (PARTITION BY source) AS source_sum_ms_played,
        LAG(ms_played, 2) OVER (PARTITION BY track_id, source ORDER BY played_at, id) AS prev2_ms_played
    FROM valid
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_max_ms_played,
        s.rn,
        s.source_sum_ms_played,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM metrics s
    JOIN listeners l ON l.id = s.listener_id
),
device_stats AS (
    -- Median listener_birth_year per device
    SELECT device, PERCENTILE_APPROX(listener_birth_year, 0.5) AS median_listener_birth_year
    FROM with_details_listener
    GROUP BY device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_listener_birth_year,
        ROUND(s.listener_birth_year - g.median_listener_birth_year, 2) AS listener_birth_year_minus_median
    FROM with_details_listener s
    JOIN device_stats g ON g.device = s.device
    WHERE s.listener_birth_year >= g.median_listener_birth_year
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_device_track_id_by_listener AS listener_birth_year_minus_median_seq,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id
    FROM vs_group
    WINDOW w_device_track_id_by_listener AS (PARTITION BY device, track_id ORDER BY listener_birth_year_minus_median NULLS LAST, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, source_max_ms_played, listener_birth_year_minus_median_seq, ms_played, source_sum_ms_played
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (source_max_ms_played, listener_birth_year_minus_median_seq, ms_played, source_sum_ms_played)
) unp
ORDER BY id, metric_name
