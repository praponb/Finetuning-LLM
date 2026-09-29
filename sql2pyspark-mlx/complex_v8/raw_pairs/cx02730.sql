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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
latest_per_key AS (
    -- Keep the earliest row per adjuster_id, product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, product ORDER BY claimed_amount, id) AS row_num
        FROM covered
    ) ranked
    WHERE row_num = 1
)
SELECT id, metric_name, reading
FROM (
    SELECT id, claimed_amount, premium
    FROM latest_per_key
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (claimed_amount, premium)
) unp
ORDER BY id, metric_name
