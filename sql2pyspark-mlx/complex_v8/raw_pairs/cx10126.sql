WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        product,
        premium,
        ROUND(CUME_DIST() OVER w_claim_date_month_status_by_c, 3) AS claim_date_cume_dist,
        DENSE_RANK() OVER w_claim_date_month_status_by_c_2 AS claim_date_dense_rank,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY MONTH(claim_date), status ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount
    FROM matched
    WINDOW w_claim_date_month_status_by_c AS (PARTITION BY MONTH(claim_date), status ORDER BY claim_date), w_claim_date_month_status_by_c_2 AS (PARTITION BY MONTH(claim_date), status ORDER BY claim_date, id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.status,
        s.claim_date_cume_dist,
        s.second_approved_amount,
        p.product AS policy_product,
        p.start_date AS policy_start_date,
        po.holder_name,
        po.joined_on AS holder_joined_on
    FROM sequenced s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    policy_id,
    status,
    policy_product,
    holder_name,
    holder_joined_on,
    DATE_FORMAT(policy_start_date, 'yyyy-MM-dd') AS policy_start_date_label
FROM combined_details
WHERE UPPER(TRIM(holder_name)) LIKE 'D%'
ORDER BY status
