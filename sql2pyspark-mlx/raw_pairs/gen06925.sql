WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS val
),
scored AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_cume_dist
    FROM lines
),
matched_keys AS (
    -- Set difference on keys
    SELECT currency, id
    FROM scored
    EXCEPT
    SELECT currency, id
    FROM price_tiers
    WHERE currency != 'EUR'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.currency = k.currency AND s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(currency) AS currency_lower
    FROM verified
    WHERE currency IN ('USD', 'EUR', 'THB')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_dense_rank,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        STDDEV_SAMP(val_cume_dist) OVER (PARTITION BY currency) AS currency_stddev_samp_val_cume_dist
    FROM base
    WHERE plan NOT IN ('starter', 'enterprise', 'pro')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(plan) AS plan_length
    FROM enriched
    WHERE currency_stddev_samp_val_cume_dist > 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        val,
        val_cume_dist_dense_rank,
        val_dense_rank,
        currency_stddev_samp_val_cume_dist,
        CEIL(currency_stddev_samp_val_cume_dist) AS currency_stddev_samp_val_cume_dist_whole,
        val_cume_dist_dense_rank - currency_stddev_samp_val_cume_dist AS val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist
    FROM valid
    WHERE val_dense_rank >= 3
),
daily AS (
    -- Grouped metrics
    SELECT
        currency,
        CASE WHEN val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist >= 80 THEN 'LARGE' ELSE 'SMALL' END AS val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist_tier,
        ROUND(AVG(currency_stddev_samp_val_cume_dist), 2) AS avg_currency_stddev_samp_val_cume_dist,
        COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        ROUND(AVG(val_dense_rank), 2) AS avg_val_dense_rank,
        SUM(val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist) FILTER (WHERE plan = 'pro') AS pro_total_val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist
    FROM cleaned
    GROUP BY currency, CASE WHEN val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist >= 80 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    currency,
    val_cume_dist_dense_rank_minus_currency_stddev_samp_val_cume_dist_tier,
    avg_currency_stddev_samp_val_cume_dist,
    enterprise_count,
    distinct_ids,
    avg_val_dense_rank,
    CASE WHEN currency RLIKE '^(EUR|THB)$' THEN 'PRIORITY' WHEN currency RLIKE '^(USD)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS currency_zone
FROM daily
ORDER BY distinct_ids, avg_currency_stddev_samp_val_cume_dist;
