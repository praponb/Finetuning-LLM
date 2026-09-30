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
scored AS (
    -- Derive helper columns
    SELECT
        *,
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        CONCAT_WS('-', status, product) AS status_product_label,
        CASE WHEN claimed_amount >= 32441.2 THEN 'large' WHEN claimed_amount >= 18478.3 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM covered
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scored s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.status = 'PAID'
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        status,
        adjuster_id,
        claim_date_month_start,
        claimed_amount_tier,
        ROUND(premium - AVG(premium) OVER (PARTITION BY policy_id), 2) AS premium_vs_policy_id_avg,
        LAG(claim_date_month_start) OVER (PARTITION BY policy_id ORDER BY premium, id) AS prev_claim_date_month_start,
        premium - LAG(premium) OVER (PARTITION BY policy_id ORDER BY premium, id) AS premium_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium ASC), 3) AS premium_percent_rank,
        FIRST_VALUE(claim_date_month_start) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date_month_start
    FROM qualified
)
SELECT
    id,
    claim_date,
    status,
    adjuster_id,
    claim_date_month_start,
    claimed_amount_tier,
    prev_claim_date_month_start,
    premium_change,
    first_known_claim_date_month_start
FROM windowed
WHERE prev_claim_date_month_start IS NOT NULL
