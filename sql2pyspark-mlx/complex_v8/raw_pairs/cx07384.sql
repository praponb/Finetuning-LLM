WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CASE WHEN status IN ('LAPSED', 'CANCELLED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE holder_id IS NOT NULL
      AND end_date IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        LAG(premium, 2) OVER (PARTITION BY holder_id, status ORDER BY premium DESC, id) AS prev2_premium,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium
    FROM recent
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.risk_score AS holder_risk_score,
        p.holder_name,
        p.segment AS holder_segment
    FROM sequenced s
    JOIN policyholders p ON p.id = s.holder_id
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.prev2_premium,
        s.holder_risk_score,
        s.holder_name,
        s.holder_segment,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount
    FROM with_details_holder s
    JOIN claims c ON c.policy_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        product,
        end_date,
        status,
        holder_risk_score,
        holder_segment,
        claim_id,
        claim_reported_at,
        claim_claimed_amount,
        SUBSTRING(TRIM(holder_segment), 1, 3) AS holder_segment_prefix,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label,
        ADD_MONTHS(end_date, 3) AS end_date_plus_3m
    FROM exploded_detail_claims
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE holder_risk_score > (SELECT AVG(holder_risk_score) FROM derived)
),
joined AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.end_date,
        s.status,
        s.claim_id,
        s.claim_reported_at,
        s.claim_claimed_amount,
        s.holder_segment_prefix,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM qualified s
    JOIN claims c ON c.id = s.claim_id AND c.claim_date < DATE '2025-06-24'
    WHERE c.adjuster_id IS NOT NULL
)
SELECT
    claim_adjuster_id,
    status,
    claim_id,
    SUM(CASE WHEN claim_status = 'REJECTED' THEN claim_claimed_amount ELSE 0 END) AS claim_claimed_amount_claim_status_rejected,
    COUNT(*) AS row_count
FROM joined
GROUP BY CUBE(claim_adjuster_id, status, claim_id)
ORDER BY claim_adjuster_id NULLS LAST, status NULLS LAST, claim_id NULLS LAST
