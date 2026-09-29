WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM covered
    WHERE premium < (SELECT MAX(premium) FROM covered)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        status,
        product,
        premium,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM without_matches
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT product, status, premium_bucket
    FROM scored
) src
PIVOT (
    AVG(premium_bucket)
    FOR status IN ('OPEN', 'APPROVED', 'IN_REVIEW')
)
ORDER BY product
