WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
base AS (
    SELECT *, DATE_TRUNC('year', effective_from) AS effective_from_period
    FROM exploded
    WHERE effective_from >= DATE '2024-06-10'
),
kept_keys AS (
    SELECT currency
    FROM base
    INTERSECT ALL
    SELECT currency
    FROM price_tiers
    WHERE plan = 'basic'
),
reconciled AS (
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.currency = k.currency
),
metrics AS (
    SELECT
        *,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value,
        MAX(tier_value) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS tier_value_max_7d,
        COUNT(tier_value) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS tier_value_count_30d
    FROM reconciled
),
scored AS (
    SELECT
        effective_from,
        effective_from_period,
        currency_avg_tier_value,
        tier_value_max_7d,
        MIN(tier_value_max_7d) OVER w_running AS currency_min_tier_value_max_7d,
        COUNT(tier_value) OVER w_running_2 AS tier_value_count_90d,
        SUM(tier_value) OVER w_running AS currency_sum_tier_value
    FROM metrics
    WHERE tier_value <= 38
    WINDOW w_running AS (PARTITION BY currency), w_running_2 AS (PARTITION BY MONTH(effective_from_period), currency ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        SUM(tier_value_max_7d) OVER w_recent AS tier_value_max_7d_sum_90d,
        MAX(currency_min_tier_value_max_7d) OVER w_recent AS currency_min_tier_value_max_7d_max_90d,
        AVG(currency_avg_tier_value) OVER w_recent AS currency_avg_tier_value_avg_90d
    FROM scored
    WINDOW w_recent AS (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
scored_price_tiers AS (
    SELECT
        *,
        SUM(tier_value_max_7d) OVER w_ordered AS tier_value_max_7d_sum_7d,
        MAX(tier_value_max_7d) OVER w_ordered AS tier_value_max_7d_max_7d,
        CUME_DIST() OVER w_hist AS tier_value_max_7d_cume_dist,
        SUM(tier_value_max_7d_sum_90d) OVER w_ordered AS tier_value_max_7d_sum_90d_sum_7d
    FROM enriched
    WINDOW w_ordered AS (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY tier_value_max_7d DESC NULLS LAST)
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    COUNT(*) AS event_count,
    MIN(tier_value_max_7d) AS min_tier_value_max_7d,
    (UNIX_TIMESTAMP(MAX(effective_from_period)) - UNIX_TIMESTAMP(MIN(effective_from_period))) / 60 AS span_minutes
FROM scored_price_tiers
GROUP BY DATE_TRUNC('WEEK', effective_from)
HAVING (UNIX_TIMESTAMP(MAX(effective_from_period)) - UNIX_TIMESTAMP(MIN(effective_from_period))) / 60 > 0
