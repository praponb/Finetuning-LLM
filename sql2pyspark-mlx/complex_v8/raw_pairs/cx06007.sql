WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        prior_ms_played_1d + prior_rows_1d AS prior_ms_played_1d_plus_prior_rows_1d
    FROM in_window
    WHERE prior_rows_1d >= 2
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    prior_rows_1d,
    GROUPING(listener_id) AS is_total_listener_id,
    MIN(prior_rows_1d) AS min_prior_rows_1d,
    ROUND(AVG(prior_ms_played_1d_plus_prior_rows_1d), 2) AS avg_prior_ms_played_1d_plus_prior_rows_1d,
    COUNT(DISTINCT id) AS distinct_ids
FROM annotated
GROUP BY GROUPING SETS ((listener_id, prior_rows_1d), (listener_id), (prior_rows_1d), ())
