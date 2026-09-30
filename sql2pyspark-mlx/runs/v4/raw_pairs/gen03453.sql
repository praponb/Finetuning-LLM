WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        val,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_dense_rank,
        STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val,
        COUNT(val) OVER (PARTITION BY currency, plan) AS currency_count_val
    FROM exploded
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, currency
    FROM enriched
    INTERSECT
    SELECT id, currency
    FROM price_tiers
    WHERE currency IN ('USD', 'THB', 'EUR')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.currency = k.currency
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        currency,
        val,
        val_dense_rank,
        plan_stddev_samp_val,
        currency_count_val,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY currency_count_val NULLS LAST) AS currency_count_val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank DESC NULLS LAST) AS val_dense_rank_percent_rank,
        STDDEV_SAMP(plan_stddev_samp_val) OVER (PARTITION BY currency) AS currency_stddev_samp_plan_stddev_samp_val
    FROM reconciled
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        val,
        val_dense_rank,
        plan_stddev_samp_val,
        currency_count_val,
        currency_count_val_percent_rank,
        val_dense_rank_percent_rank,
        currency_stddev_samp_plan_stddev_samp_val,
        MAX(currency_count_val_percent_rank) OVER (PARTITION BY currency) AS currency_max_currency_count_val_percent_rank,
        ROUND(currency_count_val / SUM(currency_count_val) OVER (PARTITION BY plan), 4) AS currency_count_val_share,
        AVG(val) OVER w_running AS plan_avg_val
    FROM enriched_price_tiers
    WINDOW w_running AS (PARTITION BY plan)
)
SELECT
    id,
    plan,
    currency,
    val,
    val_dense_rank,
    plan_stddev_samp_val,
    currency_count_val,
    currency_count_val_percent_rank,
    currency_stddev_samp_plan_stddev_samp_val,
    CASE WHEN currency RLIKE '^(EUR|THB)$' THEN 'CORE' ELSE 'EXTENDED' END AS currency_zone
FROM metrics
ORDER BY currency_count_val DESC
