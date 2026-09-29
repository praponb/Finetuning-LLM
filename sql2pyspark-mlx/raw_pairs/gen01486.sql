WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(val) OVER w_hist AS currency_sum_val,
        MAX(val) OVER w_hist AS currency_max_val
    FROM lines
    WINDOW w_hist AS (PARTITION BY currency)
),
kept_keys AS (
    -- Set difference on keys
    SELECT currency, id
    FROM metrics
    EXCEPT
    SELECT currency, id
    FROM price_tiers
    WHERE plan = 'enterprise'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.currency = k.currency AND s.id = k.id
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        currency,
        currency_sum_val,
        currency_max_val,
        SUM(currency_max_val) OVER (PARTITION BY MONTH(effective_from)) AS effective_from_month_sum_currency_max_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
        STDDEV_SAMP(currency_max_val) OVER (PARTITION BY currency) AS currency_stddev_samp_currency_max_val,
        RANK() OVER (ORDER BY currency_max_val) AS currency_max_val_rank,
        STDDEV_SAMP(currency_sum_val) OVER (PARTITION BY currency) AS currency_stddev_samp_currency_sum_val
    FROM reconciled
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(currency_stddev_samp_currency_sum_val, 2) AS currency_stddev_samp_currency_sum_val_rounded
    FROM metrics_price_tiers
    WHERE val_percent_rank <= 0.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY currency_stddev_samp_currency_max_val DESC NULLS LAST) AS currency_stddev_samp_currency_max_val_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_sum_val DESC) AS currency_sum_val_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank NULLS LAST) AS val_percent_rank_percent_rank
    FROM valid
    WHERE currency_stddev_samp_currency_sum_val_rounded <= 55
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        currency,
        currency_sum_val,
        currency_max_val,
        effective_from_month_sum_currency_max_val,
        val_percent_rank,
        currency_stddev_samp_currency_max_val,
        currency_max_val_rank,
        currency_stddev_samp_currency_sum_val_rounded,
        currency_stddev_samp_currency_max_val_cume_dist,
        val_percent_rank_percent_rank,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM with_windows
    WHERE effective_from > TO_DATE('2024-02-24')
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', effective_from) AS week_start,
        PERCENTILE_APPROX(val_percent_rank_percent_rank, 0.5) AS val_percent_rank_percent_rank_p50,
        COUNT_IF(currency = 'USD') AS usd_events,
        PERCENTILE_APPROX(currency_max_val, 0.5) AS currency_max_val_p50,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered
    GROUP BY DATE_TRUNC('WEEK', effective_from)
    HAVING PERCENTILE_APPROX(val_percent_rank_percent_rank, 0.5) > 0
)
SELECT
    week_start,
    usd_events,
    currency_max_val_p50,
    distinct_ids
FROM summary;
