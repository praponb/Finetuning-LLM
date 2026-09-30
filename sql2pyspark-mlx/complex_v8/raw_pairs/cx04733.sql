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
with_parent_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.product,
        s.premium,
        p.holder_id AS policy_holder_id,
        po.holder_name,
        po.joined_on AS holder_joined_on
    FROM matched s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.holder_id IS NOT NULL
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_holder_id,
        product,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COUNT(DISTINCT product) AS distinct_products,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        ROUND(SUM(premium) / NULLIF(SUM(claimed_amount), 0), 3) AS premium_to_claimed_amount_ratio,
        COUNT(DISTINCT policy_holder_id) AS distinct_policy_holder_ids,
        COUNT(*) AS cnt
    FROM with_parent_policy
    GROUP BY policy_holder_id, product, DATE_TRUNC('MONTH', claim_date)
),
annotated AS (
    -- Label and score rows
    SELECT
        policy_holder_id,
        product,
        claim_date_month,
        status_list,
        cnt,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DAYOFWEEK(claim_date_month) AS claim_date_month_dayofweek
    FROM stats
    WHERE premium_to_claimed_amount_ratio > 4.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_holder_id, product, field, metric_value
FROM (
    SELECT policy_holder_id, product, cnt, claim_date_month_dayofweek
    FROM annotated
) src
UNPIVOT (
    metric_value FOR field IN (cnt, claim_date_month_dayofweek)
) unp
ORDER BY policy_holder_id, product, field
