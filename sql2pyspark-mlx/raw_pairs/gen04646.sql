WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS element
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        PERCENT_RANK() OVER w_hist AS element_percent_rank,
        DENSE_RANK() OVER w_ordered AS element_dense_rank
    FROM lines
    WHERE effective_from < '2024-12-01'
    WINDOW w_hist AS (ORDER BY element DESC NULLS FIRST), w_ordered AS (ORDER BY element DESC)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        element_percent_rank,
        element_dense_rank,
        DENSE_RANK() OVER (ORDER BY element_percent_rank) AS element_percent_rank_dense_rank
    FROM enriched
),
scored AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (ORDER BY element_dense_rank DESC NULLS FIRST) AS element_dense_rank_cume_dist
    FROM metrics
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        element_percent_rank,
        element_percent_rank_dense_rank,
        element_percent_rank_dense_rank + element_dense_rank_cume_dist AS element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist
    FROM scored
    WHERE element_dense_rank_cume_dist BETWEEN 0.4 AND 0.46
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT *, RANK() OVER w_running AS element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist_rank
    FROM cleaned
    WINDOW w_running AS (ORDER BY element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist DESC NULLS LAST)
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        element_percent_rank,
        element_percent_rank_dense_rank,
        element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist,
        element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist_rank,
        CAST(element_percent_rank AS INT) AS element_percent_rank_int
    FROM scored_price_tiers
    WHERE element_percent_rank_dense_rank = 7
)
SELECT
    id,
    element_percent_rank,
    element_percent_rank_dense_rank,
    element_percent_rank_dense_rank_plus_element_dense_rank_cume_dist,
    element_percent_rank_int
FROM cleaned_price_tiers
ORDER BY id DESC;
