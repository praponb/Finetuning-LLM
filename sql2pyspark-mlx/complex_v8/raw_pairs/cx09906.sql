WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        ROUND(STDDEV(holder_risk_score), 2) AS stddev_holder_risk_score
    FROM with_parent_holder
    WHERE premium < 4678.8
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
    HAVING MAX(premium) > 2033.8
),
metrics AS (
    -- Previous / next values per key
    SELECT
        end_date_ym,
        median_premium,
        stddev_holder_risk_score,
        SUM(median_premium) OVER (ORDER BY median_premium DESC, end_date_ym ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS median_premium_sum_6_prior,
        stddev_holder_risk_score - LAG(stddev_holder_risk_score) OVER (ORDER BY median_premium DESC, end_date_ym) AS stddev_holder_risk_score_change,
        SUM(median_premium) OVER (ORDER BY stddev_holder_risk_score ASC, end_date_ym ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS median_premium_sum_7,
        COUNT(*) OVER (ORDER BY stddev_holder_risk_score DESC, end_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER w_all_by_median_premium AS median_premium_seq
    FROM totals
    WINDOW w_all_by_median_premium AS (ORDER BY median_premium DESC, end_date_ym)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE stddev_holder_risk_score_change > (SELECT AVG(stddev_holder_risk_score_change) FROM metrics)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        end_date_ym,
        median_premium_sum_6_prior,
        stddev_holder_risk_score_change,
        median_premium_sum_7,
        median_premium_seq,
        LAST_VALUE(running_count) OVER (PARTITION BY median_premium_seq ORDER BY running_count, end_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_running_count,
        running_count - LAG(running_count) OVER (PARTITION BY median_premium_seq ORDER BY running_count, end_date_ym) AS running_count_change
    FROM screened
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE stddev_holder_risk_score_change > (SELECT AVG(stddev_holder_risk_score_change) FROM with_trends)
)
-- Final projection
SELECT
    end_date_ym,
    median_premium_sum_6_prior,
    median_premium_seq,
    last_running_count,
    SUBSTRING(TRIM(end_date_ym), 1, 3) AS end_date_ym_prefix
FROM without_matches
ORDER BY median_premium_sum_6_prior, end_date_ym
LIMIT 10
