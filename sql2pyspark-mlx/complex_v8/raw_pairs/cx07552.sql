WITH covered AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id, product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_id, product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        LEAD(claimed_amount, 3) OVER (PARTITION BY policy_id, product ORDER BY premium, id) AS next3_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY policy_id, product ORDER BY premium, id) AS seq_no
    FROM covered
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        approved_amount,
        product,
        premium,
        second_claim_date,
        first_claim_date,
        next3_claimed_amount,
        seq_no,
        MONTH(claim_date) AS claim_date_month,
        DATE_TRUNC('WEEK', second_claim_date) AS second_claim_date_week,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM windowed
    WHERE first_claim_date IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY MONTH(second_claim_date))) AS distinct_product_in_second_claim_date_month,
        NTILE(3) OVER (PARTITION BY product, MONTH(first_claim_date) ORDER BY claimed_amount DESC, id) AS claimed_amount_tercile,
        SIZE(COLLECT_SET(status_or_unknown) OVER (PARTITION BY MONTH(second_claim_date))) AS distinct_status_or_unknown_in_second_claim_date_
    FROM annotated
)
SELECT
    id,
    claim_date,
    claimed_amount,
    approved_amount,
    product,
    premium,
    second_claim_date,
    first_claim_date,
    seq_no,
    second_claim_date_week,
    status_or_unknown,
    distinct_product_in_second_claim_date_month,
    claimed_amount_tercile
FROM with_windows
