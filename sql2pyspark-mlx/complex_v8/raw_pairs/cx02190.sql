WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(premium / NULLIF(claimed_amount, 0), 3) AS premium_per_claimed_amount
    FROM matched
)
SELECT
    COALESCE(status, 'ALL') AS status,
    COALESCE(product, 'ALL') AS product,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM tagged
GROUP BY GROUPING SETS ((status, product, claim_type), (status), (product), ())
ORDER BY status NULLS LAST, product NULLS LAST, claim_type NULLS LAST
