WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS val
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(val AS INT) AS val_int
    FROM exploded
    WHERE currency = 'EUR'
),
scored AS (
    -- Ranking and gap features
    SELECT
        val,
        val_int,
        MIN(val_int) OVER (PARTITION BY currency) AS currency_min_val_int,
        CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist,
        DENSE_RANK() OVER w_recent AS val_int_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val NULLS LAST) AS val_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC) AS val_cume_dist_2
    FROM valid
    WINDOW w_recent AS (PARTITION BY currency ORDER BY val_int NULLS LAST)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        val_int,
        currency_min_val_int,
        val_cume_dist,
        val_int_dense_rank,
        val_cume_dist_2,
        val / NULLIF(val_cume_dist_2, 0) AS val_per_val_cume_dist_2
    FROM scored
    WHERE val <= 51
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY currency_min_val_int DESC NULLS LAST) AS currency_min_val_int_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val_int DESC) AS val_int_percent_rank
    FROM base
)
SELECT
    val_int_dense_rank,
    val_cume_dist_2,
    val_per_val_cume_dist_2,
    val_cume_dist_percent_rank,
    val_int_percent_rank
FROM scored_price_tiers
ORDER BY val_per_val_cume_dist_2 DESC NULLS FIRST;
