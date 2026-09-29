WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
    WHERE start_date >= DATE '2024-11-20'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(CASE WHEN claim_type = 'fire' THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM base_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.product_prefix,
        p.risk_score AS holder_risk_score
    FROM summarized_rows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        holder_id,
        YEAR(start_date) AS start_date_year,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(holder_risk_score), 2) AS stddev_holder_risk_score,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        COUNT(*) AS cnt
    FROM with_details_holder
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM'), holder_id, YEAR(start_date)
    HAVING AVG(premium) > 1132.1
)
SELECT end_date_ym, holder_id, metric_name, reading
FROM (
    SELECT end_date_ym, holder_id, end_date_span_days, distinct_ids, start_date_year, distinct_holder_ids
    FROM summary
) src
UNPIVOT (
    reading FOR metric_name IN (end_date_span_days, distinct_ids, start_date_year, distinct_holder_ids)
) unp
