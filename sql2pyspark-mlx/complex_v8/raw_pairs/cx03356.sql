WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        policy_id,
        COUNT(*) FILTER (WHERE claim_type <> 'collision') AS n_claim_type_collision,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        MAX(claim_date) AS last_claim_date,
        SUM(CASE WHEN status IS NOT NULL THEN claimed_amount ELSE 0 END) AS claimed_amount_status_is_not_null,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM matched
    WHERE premium >= 2111.3
    GROUP BY 1, 2
),
enriched AS (
    -- Join policies
    SELECT
        s.claim_date_month,
        s.policy_id,
        s.product_list,
        s.last_claim_date,
        s.claimed_amount_status_is_not_null,
        s.claim_type_list,
        p.premium AS policy_premium
    FROM rollup_base s
    LEFT JOIN policies p ON p.id = s.policy_id
),
per_label AS (
    -- One row per product_list_item
    SELECT
        s.claim_date_month,
        s.policy_id,
        s.last_claim_date,
        s.claimed_amount_status_is_not_null,
        s.claim_type_list,
        s.policy_premium,
        t.product_list_item
    FROM enriched s
    LATERAL VIEW EXPLODE(s.product_list) t AS product_list_item
)
SELECT
    claim_date_month,
    policy_id,
    claimed_amount_status_is_not_null,
    product_list_item,
    DATEDIFF(DATE '2026-01-01', last_claim_date) AS days_since_last_claim_date,
    DATE_ADD(last_claim_date, 7) AS last_claim_date_plus_7d
FROM per_label
