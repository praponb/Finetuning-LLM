WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.premium_bucket,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment <> 'RETAIL'
    WHERE p.risk_score BETWEEN 0.2 AND 0.9
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(premium_bucket) IGNORE NULLS OVER (PARTITION BY MONTH(holder_joined_on), product ORDER BY premium_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_bucket,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY MONTH(holder_joined_on), product ORDER BY premium_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), YEAR(start_date) ORDER BY holder_joined_on NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_bucket_avg_6_prior,
        SUM(premium_bucket) OVER (PARTITION BY MONTH(holder_joined_on), product ORDER BY premium_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_bucket,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY MONTH(holder_joined_on), product ORDER BY premium_bucket ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_bucket_avg_2_prior,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), YEAR(start_date) ORDER BY holder_joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_2,
        RANK() OVER w_holder_joined_on_month_start_3 AS holder_joined_on_rank,
        RANK() OVER (PARTITION BY MONTH(holder_joined_on), product ORDER BY premium_bucket ASC) AS premium_bucket_rank,
        COUNT(*) OVER w_start_date_month AS rows_in_start_date_month,
        SUM(holder_risk_score) OVER (PARTITION BY MONTH(holder_joined_on), product ORDER BY premium_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score
    FROM joined_holder
    WINDOW w_holder_joined_on_month_start_3 AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), YEAR(start_date) ORDER BY holder_joined_on NULLS LAST, id), w_start_date_month AS (PARTITION BY MONTH(start_date))
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        product,
        start_date,
        premium_bucket,
        holder_joined_on,
        holder_risk_score,
        holder_name,
        first_known_premium_bucket,
        second_start_date,
        premium_bucket_avg_6_prior,
        running_premium_bucket,
        premium_bucket_avg_2_prior,
        second_start_date_2,
        holder_joined_on_rank,
        rows_in_start_date_month,
        running_holder_risk_score
    FROM sequenced
    WHERE premium_bucket_rank = 1
      AND start_date IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium_bucket_avg_2_prior DESC, id)) AS days_since_prev_holder_joined_on,
        COUNT(*) OVER (PARTITION BY MONTH(second_start_date_2)) AS rows_in_second_start_date_2_month,
        MIN(running_holder_risk_score) OVER (PARTITION BY MONTH(second_start_date_2)) AS second_start_date_2_month_min_running_holder_ris,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium_bucket_avg_2_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        LAST_VALUE(rows_in_start_date_month) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium_bucket_avg_2_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rows_in_start_date_month
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium_bucket,
    holder_joined_on,
    holder_risk_score,
    holder_name,
    first_known_premium_bucket,
    second_start_date,
    premium_bucket_avg_6_prior,
    running_premium_bucket,
    premium_bucket_avg_2_prior,
    rows_in_start_date_month,
    running_holder_risk_score,
    days_since_prev_holder_joined_on,
    rows_in_second_start_date_2_month,
    second_start_date_2_month_min_running_holder_ris,
    last_known_rows_in_start_date_month
FROM metrics
ORDER BY id
LIMIT 20
