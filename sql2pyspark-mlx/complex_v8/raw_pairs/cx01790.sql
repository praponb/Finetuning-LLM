WITH filtered_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 4
),
deduped AS (
    -- Keep the latest row per team
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY level DESC, id DESC) AS recency_rank
        FROM filtered_adjusters
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_name,
        team,
        level,
        NTILE(3) OVER w_all_by_level AS level_tercile,
        RANK() OVER w_all_by_level AS level_rank
    FROM deduped
    WINDOW w_all_by_level AS (ORDER BY level DESC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(level_rank / NULLIF(SUM(level_rank) OVER w_level_tercile_level_rank, 0), 4) AS level_rank_share_of_level_tercile_level_rank,
        level - LAG(level) OVER (PARTITION BY level ORDER BY level_tercile DESC, id) AS level_change,
        COUNT(*) OVER (PARTITION BY level_tercile, level_rank) AS rows_in_level_tercile_level_rank,
        SIZE(COLLECT_SET(team) OVER (PARTITION BY level_tercile, level_rank)) AS distinct_team_in_level_tercile_level_rank
    FROM with_windows
    WINDOW w_level_tercile_level_rank AS (PARTITION BY level_tercile, level_rank)
),
stats AS (
    -- Summarise per group
    SELECT level_rank, COUNT(*) AS n_rows, MAX(level) AS max_level
    FROM sequenced
    WHERE level_change <= 0
    GROUP BY level_rank
    HAVING SUM(rows_in_level_tercile_level_rank) > 5
),
annotated AS (
    -- Add calculated columns
    SELECT
        level_rank,
        n_rows,
        max_level,
        ROUND(n_rows / NULLIF(level_rank, 0), 3) AS n_rows_per_level_rank,
        CASE WHEN level_rank >= 8 THEN 'A' WHEN level_rank >= 3 THEN 'B' ELSE 'C' END AS level_rank_tier
    FROM stats
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        level_rank,
        max_level,
        n_rows_per_level_rank,
        level_rank_tier,
        n_rows_per_level_rank - LAG(n_rows_per_level_rank) OVER (PARTITION BY max_level, n_rows ORDER BY n_rows_per_level_rank, level_rank) AS n_rows_per_level_rank_change,
        NTILE(3) OVER w_max_level_n_rows_by_n_rows_p AS n_rows_per_level_rank_tercile
    FROM annotated
    WINDOW w_max_level_n_rows_by_n_rows_p AS (PARTITION BY max_level, n_rows ORDER BY n_rows_per_level_rank, level_rank)
)
-- Final projection
SELECT
    level_rank,
    level_rank_tier,
    n_rows_per_level_rank_change,
    n_rows_per_level_rank_tercile,
    n_rows_per_level_rank + max_level AS n_rows_per_level_rank_plus_max_level,
    ROUND(n_rows_per_level_rank * 1.5, 2) AS n_rows_per_level_rank_adj
FROM windowed
ORDER BY n_rows_per_level_rank_adj NULLS LAST, level_rank DESC
LIMIT 20
