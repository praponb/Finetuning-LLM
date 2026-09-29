WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
latest_per_key AS (
    -- Keep the latest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY approved_amount DESC, id DESC) AS rn
        FROM in_window
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status, policy_id ORDER BY approved_amount DESC, id) AS seq_no,
        MAX(claimed_amount) OVER (PARTITION BY status, policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        SUM(premium) OVER (PARTITION BY product, MONTH(claim_date)) AS product_claim_date_month_sum_premium
    FROM latest_per_key
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        LENGTH(TRIM(status)) AS status_len
    FROM ranked
    WHERE status = 'PAID'
)
-- Final projection
SELECT
    id,
    policy_id,
    claimed_amount,
    approved_amount,
    status,
    premium,
    max_claimed_amount_so_far,
    product_claim_date_month_sum_premium,
    product_prefix,
    status_len,
    QUARTER(claim_date) AS claim_date_quarter,
    DATE_TRUNC('MONTH', claim_date) AS claim_date_month
FROM annotated
ORDER BY status_len, premium
