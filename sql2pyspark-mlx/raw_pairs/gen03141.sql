WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element NULLS LAST) AS element_cume_dist,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank,
        PERCENT_RANK() OVER (ORDER BY element) AS element_percent_rank,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank_2,
        CUME_DIST() OVER (ORDER BY element DESC NULLS LAST) AS element_cume_dist_2
    FROM exploded
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE plan = 'starter'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_hist AS element_cume_dist_2_cume_dist,
        PERCENT_RANK() OVER (ORDER BY element_percent_rank DESC NULLS LAST) AS element_percent_rank_percent_rank,
        RANK() OVER w_hist_2 AS element_rank
    FROM kept_rows
    WINDOW w_hist AS (ORDER BY element_cume_dist_2 DESC NULLS LAST), w_hist_2 AS (ORDER BY element NULLS LAST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        element,
        element_dense_rank,
        element_percent_rank,
        element_cume_dist_2,
        element_rank,
        RANK() OVER (ORDER BY element_percent_rank_percent_rank DESC) AS element_percent_rank_percent_rank_rank,
        RANK() OVER (ORDER BY element_dense_rank DESC NULLS FIRST) AS element_dense_rank_rank,
        RANK() OVER (ORDER BY element_percent_rank) AS element_percent_rank_rank,
        DENSE_RANK() OVER (ORDER BY element_dense_rank DESC NULLS LAST) AS element_dense_rank_dense_rank
    FROM scored
)
SELECT
    id,
    element_percent_rank,
    element_cume_dist_2,
    element_rank,
    element_dense_rank_rank,
    element_percent_rank_rank,
    element_dense_rank_dense_rank
FROM metrics
ORDER BY element_cume_dist_2 NULLS FIRST
