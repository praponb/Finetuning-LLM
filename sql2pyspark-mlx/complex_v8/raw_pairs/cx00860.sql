WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE NOT is_skipped
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        NTILE(5) OVER (ORDER BY played_at ASC, id) AS played_at_quintile
    FROM scoped_plays
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', last_known_played_at) AS last_known_played_at_month,
        source,
        device,
        PERCENTILE_APPROX(played_at_quintile, 0.5) AS median_played_at_quintile,
        ROUND(AVG(played_at_quintile), 2) AS avg_played_at_quintile
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', last_known_played_at), source, device
    HAVING MAX(played_at_quintile) > 5
)
-- Final projection
SELECT
    last_known_played_at_month,
    source,
    device,
    median_played_at_quintile,
    avg_played_at_quintile
FROM rollup_base
WHERE device IS NOT NULL
ORDER BY avg_played_at_quintile
