WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        COALESCE(premium, 0.0) AS premium_filled,
        ABS(approved_amount - 5336.7) AS approved_amount_distance
    FROM joined_ranges
    WHERE product IN ('LIFE', 'TRAVEL')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(reported_at) AS last_claim_reported_at,
        MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        c.first_claim_reported_at
    FROM scored s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
unique_rows AS (
    -- Keep the earliest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        approved_amount,
        product,
        premium,
        claim_type_len,
        premium_filled,
        approved_amount_distance,
        last_claim_reported_at,
        first_claim_reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY approved_amount_distance, id) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
)
SELECT *
FROM (
    SELECT policy_id, claim_type, premium_filled
    FROM unique_rows
) src
PIVOT (
    SUM(premium_filled)
    FOR claim_type IN ('theft', 'delay', 'flood', 'collision')
)
ORDER BY policy_id
