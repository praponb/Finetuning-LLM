WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE product <> 'TRAVEL'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.region AS holder_region,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_filled,
        s.holder_segment,
        s.holder_risk_score,
        p.holder_name,
        p.segment AS holder_segment_2
    FROM joined_holder s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment = 'RETAIL'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(premium_filled) OVER w_status_holder_id AS status_holder_id_sum_premium_filled,
        ROUND(PERCENT_RANK() OVER (ORDER BY holder_risk_score DESC NULLS FIRST, id), 3) AS holder_risk_score_percent_rank,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY status, holder_id), 0), 4) AS holder_risk_score_share_of_status_holder_id,
        LAG(end_date) OVER (ORDER BY end_date NULLS LAST, id) AS prev_end_date
    FROM with_parent_holder
    WINDOW w_status_holder_id AS (PARTITION BY status, holder_id)
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.premium_filled,
        s.holder_segment_2,
        s.status_holder_id_sum_premium_filled,
        s.holder_risk_score_percent_rank,
        s.holder_risk_score_share_of_status_holder_id,
        s.prev_end_date,
        c.id AS claim_id,
        c.claim_type,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount
    FROM windowed s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    end_date,
    premium_filled,
    holder_segment_2,
    status_holder_id_sum_premium_filled,
    holder_risk_score_share_of_status_holder_id,
    prev_end_date,
    claim_type,
    claim_approved_amount,
    claim_claimed_amount,
    ADD_MONTHS(prev_end_date, 12) AS prev_end_date_plus_12m,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM detail_rows
WHERE claim_id <= 24
ORDER BY status_holder_id_sum_premium_filled DESC, prev_end_date DESC
