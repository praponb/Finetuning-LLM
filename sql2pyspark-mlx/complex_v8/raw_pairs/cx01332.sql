WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prior_rows_7d / NULLIF(SUM(prior_rows_7d) OVER (PARTITION BY prior_rows_7d, listener_id), 0), 4) AS prior_rows_7d_share_of_prior_rows_7d_listener_id,
        MAX(prior_rows_7d) OVER (PARTITION BY prior_rows_7d, listener_id) AS prior_rows_7d_listener_id_max_prior_rows_7d
    FROM matched
),
stats AS (
    -- Summarise per group
    SELECT
        prior_rows_7d,
        listener_id,
        MIN(prior_rows_7d) AS min_prior_rows_7d,
        COUNT(*) AS row_count,
        MAX(played_at) AS last_played_at
    FROM with_windows
    GROUP BY prior_rows_7d, listener_id
),
filtered AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.plan = 'premium'
        )
)
-- Final projection
SELECT
    prior_rows_7d,
    listener_id,
    min_prior_rows_7d,
    row_count,
    last_played_at,
    row_count + min_prior_rows_7d AS row_count_plus_min_prior_rows_7d
FROM filtered
