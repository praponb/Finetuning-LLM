WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month
    FROM claims
    WHERE (status NOT IN ('OPEN', 'PAID') OR policy_id IS NOT NULL)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date
    FROM scoped_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type_or_unknown,
        DATE_TRUNC('MONTH', claim_date_month) AS claim_date_month_month,
        MAX(approved_amount) AS max_approved_amount,
        COUNT(*) FILTER (WHERE claim_type <> 'medical') AS n_claim_type_medical,
        MIN(approved_amount) AS min_approved_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM with_stats
    WHERE claim_type IS NOT NULL
    GROUP BY claim_type_or_unknown, DATE_TRUNC('MONTH', claim_date_month)
    HAVING SUM(claimed_amount) > 4868.7
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        n_claim_type_medical - LAG(n_claim_type_medical) OVER (ORDER BY max_approved_amount DESC, claim_type_or_unknown, claim_date_month_month) AS n_claim_type_medical_change,
        NTH_VALUE(max_approved_amount, 2) OVER (ORDER BY max_approved_amount DESC, claim_type_or_unknown, claim_date_month_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_approved_amount,
        SIZE(COLLECT_SET(claim_type_or_unknown) OVER (PARTITION BY n_claim_type_medical)) AS distinct_claim_type_or_unknown_in_n_claim_type_m
    FROM per_group
)
-- Native SQL UNPIVOT (wide to long)
SELECT claim_type_or_unknown, claim_date_month_month, metric_name, metric_value
FROM (
    SELECT claim_type_or_unknown, claim_date_month_month, n_claim_type_medical_change, distinct_claim_type_or_unknown_in_n_claim_type_m
    FROM with_windows
) src
UNPIVOT (
    metric_value FOR metric_name IN (n_claim_type_medical_change, distinct_claim_type_or_unknown_in_n_claim_type_m)
) unp
