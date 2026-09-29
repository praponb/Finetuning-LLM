WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
deduped AS (
    -- Keep the latest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        approved_amount,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
),
joined_policy AS (
    -- Join policies
    SELECT s.*, p.product AS policy_product
    FROM deduped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN product IN ('AUTO', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        CASE WHEN claim_type IN ('fire', 'flood') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        CONCAT_WS('-', claim_type, policy_product) AS claim_type_policy_product_label
    FROM joined_policy
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(policy_product, 'ALL') AS policy_product,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING(policy_product) AS is_total_policy_product,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ARRAY_SORT(COLLECT_SET(policy_product)) AS policy_product_list
FROM annotated
GROUP BY GROUPING SETS ((policy_product, claim_type), (policy_product))
