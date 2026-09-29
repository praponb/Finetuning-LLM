WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 3465.4) AS premium_distance,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM policies
    WHERE holder_id IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount,
        MAX(reported_at) AS last_claim_reported_at,
        COUNT(CASE WHEN claim_type = 'theft' THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_claimed_amount,
        c.last_claim_reported_at,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM scoped s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', end_date_label, status) AS end_date_label_status_label,
        ROUND(premium_distance * 1.07, 2) AS premium_distance_adj,
        COALESCE(premium, 0.0) AS premium_filled
    FROM summarized_rows
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.premium_distance,
        s.start_date_month,
        s.end_date_label,
        s.avg_claim_claimed_amount,
        s.last_claim_reported_at,
        s.claim_claim_type_count,
        s.premium_distance_adj,
        p.risk_score AS holder_risk_score
    FROM scored s
    JOIN policyholders p ON p.id = s.holder_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ADD_MONTHS(end_date, 1) AS end_date_plus_1m,
        CASE WHEN premium_distance >= 3750.0 THEN 'large' WHEN premium_distance >= 1750.0 THEN 'medium' ELSE 'small' END AS premium_distance_tier,
        ROUND(avg_claim_claimed_amount / NULLIF(holder_risk_score, 0), 3) AS avg_claim_claimed_amount_per_holder_risk_score
    FROM joined_holder
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        MIN(claimed_amount) AS min_claim_claimed_amount_2
    FROM claims
    GROUP BY policy_id
),
summarized_rows_2 AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.min_claim_claimed_amount,
        c.min_claim_claimed_amount_2
    FROM labeled s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    claim_count,
    holder_id,
    claim_claim_type_count,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT status) AS distinct_status
FROM summarized_rows_2
GROUP BY GROUPING SETS ((claim_count, holder_id, claim_claim_type_count), (claim_count), ())
