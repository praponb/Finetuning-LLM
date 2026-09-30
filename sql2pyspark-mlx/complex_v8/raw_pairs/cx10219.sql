WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix
    FROM plays
    WHERE track_id <= 7
),
unique_rows AS (
    -- Keep the latest row per source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        played_at_plus_1d,
        source_slug,
        source_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY played_at_plus_1d DESC, id DESC) AS recency_rank
        FROM recent_plays
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(played_at) OVER (ORDER BY played_at DESC, id) AS prev_played_at,
        LAG(played_at_plus_1d, 2) OVER (PARTITION BY source ORDER BY played_at ASC, id) AS prev2_played_at_plus_1d,
        LAG(played_at_plus_1d) OVER (ORDER BY played_at DESC, id) AS prev_played_at_plus_1d,
        DENSE_RANK() OVER w_source_by_played_at_ties AS played_at_dense_rank,
        LEAD(played_at) OVER (PARTITION BY source ORDER BY played_at ASC, id) AS next_played_at,
        RANK() OVER (PARTITION BY source ORDER BY played_at ASC) AS played_at_rank
    FROM unique_rows
    WINDOW w_source_by_played_at_ties AS (PARTITION BY source ORDER BY played_at ASC)
),
source_stats AS (
    -- Max played_at_dense_rank per source
    SELECT source, MAX(played_at_dense_rank) AS max_played_at_dense_rank
    FROM ranked
    GROUP BY source
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_played_at_dense_rank,
        ROUND(s.played_at_dense_rank - g.max_played_at_dense_rank, 2) AS played_at_dense_rank_minus_max
    FROM ranked s
    JOIN source_stats g ON g.source = s.source
    WHERE s.played_at_dense_rank = g.max_played_at_dense_rank
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prev_played_at) OVER w_device_by_next_played_at AS next_prev_played_at,
        ROUND((UNIX_TIMESTAMP(prev_played_at) - UNIX_TIMESTAMP(LAG(prev_played_at) OVER (PARTITION BY played_at_rank ORDER BY played_at, id))) / 60, 1) AS minutes_since_prev_prev_played_at,
        SUM(played_at_rank) OVER (PARTITION BY device ORDER BY next_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_played_at_rank
    FROM outliers
    WINDOW w_device_by_next_played_at AS (PARTITION BY device ORDER BY next_played_at DESC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    source,
    source_slug,
    source_prefix,
    prev2_played_at_plus_1d,
    prev_played_at_plus_1d,
    played_at_dense_rank,
    next_played_at,
    played_at_rank,
    played_at_dense_rank_minus_max,
    next_prev_played_at,
    running_played_at_rank,
    SUBSTRING(TRIM(source_prefix), 1, 3) AS source_prefix_prefix
FROM metrics
ORDER BY source_prefix_prefix, id
LIMIT 10
