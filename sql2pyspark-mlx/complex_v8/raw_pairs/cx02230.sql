WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        UPPER(TRIM(product)) AS product_clean,
        MONTH(end_date) AS end_date_month,
        UPPER(TRIM(status)) AS status_clean
    FROM policies
    WHERE product IS NOT NULL
      AND holder_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(premium) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id) AS next_premium,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        NTH_VALUE(premium, 2) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        LEAD(premium, 3) OVER (PARTITION BY MONTH(start_date) ORDER BY start_date DESC, id) AS next3_premium
    FROM recent_policys
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        s.end_date_month,
        s.next_premium,
        s.premium_avg_6_prior,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM ranked s
    JOIN claims c ON c.policy_id = s.id
),
with_details_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        s.end_date_month,
        s.next_premium,
        s.premium_avg_6_prior,
        s.claim_id,
        s.claim_reported_at,
        c.reported_at AS claim_reported_at_2
    FROM line_level_claims s
    JOIN claims c ON c.id = s.claim_id
),
enriched_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        s.end_date_month,
        s.next_premium,
        s.premium_avg_6_prior,
        s.claim_id,
        s.claim_reported_at,
        s.claim_reported_at_2,
        c.claim_date,
        c.policy_id AS claim_policy_id
    FROM with_details_claim s
    LEFT JOIN claims c ON c.id = s.claim_id
),
joined AS (
    -- Join policies
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        s.end_date_month,
        s.next_premium,
        s.premium_avg_6_prior,
        s.claim_id,
        s.claim_policy_id,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM enriched_claim s
    JOIN policies p ON p.id = s.claim_policy_id
    WHERE p.product <> 'HOME'
)
SELECT id, claim_id, metric, reading
FROM (
    SELECT id, claim_id, premium_avg_6_prior, next_premium
    FROM joined
) src
UNPIVOT (
    reading FOR metric IN (premium_avg_6_prior, next_premium)
) unp
