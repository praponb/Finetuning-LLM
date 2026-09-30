WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'LIFE'
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium
    FROM filtered_policys s
),
status_stats AS (
    -- Median premium per status
    SELECT status, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM with_counts
    GROUP BY status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM with_counts s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium >= g.median_premium
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.median_premium,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM vs_group s
    JOIN policyholders p ON p.id = s.holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM joined_holder s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        median_premium,
        holder_segment,
        holder_risk_score,
        holder_joined_on,
        policy_count,
        ROW_NUMBER() OVER (PARTITION BY MONTH(holder_joined_on), MONTH(end_date) ORDER BY holder_joined_on, id) AS holder_joined_on_seq,
        NTILE(3) OVER (ORDER BY policy_count DESC, id) AS policy_count_tercile,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, YEAR(holder_joined_on))) AS distinct_holder_id_in_product_holder_joined_on_y,
        FIRST_VALUE(holder_risk_score) OVER (PARTITION BY holder_segment, DATE_TRUNC('MONTH', start_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_risk_score,
        NTILE(4) OVER (PARTITION BY MONTH(holder_joined_on), MONTH(end_date) ORDER BY holder_joined_on, id) AS holder_joined_on_quartile,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY product, YEAR(holder_joined_on)), 0), 4) AS holder_risk_score_share_of_product_holder_joined
    FROM summarized_rows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT start_date, policy_count_tercile, holder_segment, holder_joined_on_seq
    FROM with_windows
) src
PIVOT (
    COUNT(holder_joined_on_seq)
    FOR holder_segment IN ('RETAIL', 'VIP', 'SME')
)
