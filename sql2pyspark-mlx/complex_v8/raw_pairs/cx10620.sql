WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
unique_rows AS (
    -- Keep the earliest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY approved_amount, id) AS recency_rank
        FROM matched
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount,
    adjuster_id,
    premium,
    INITCAP(TRIM(product)) AS product_clean,
    DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
FROM unique_rows
ORDER BY claim_date
