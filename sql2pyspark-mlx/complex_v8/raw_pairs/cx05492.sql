WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.approved_amount,
        s.product,
        a.adjuster_name
    FROM covered s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        adjuster_id,
        approved_amount,
        product,
        adjuster_name,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY product, policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_max_1_prior
    FROM joined
),
rollup_base AS (
    -- Summarise per group
    SELECT
        product,
        adjuster_id,
        policy_id,
        ROUND(SUM(approved_amount_max_1_prior) / NULLIF(SUM(claimed_amount), 0), 3) AS approved_amount_max_1_prior_to_claimed_amount_ra,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        MAX(approved_amount_max_1_prior) AS max_approved_amount_max_1_prior,
        SUM(CASE WHEN adjuster_name IS NOT NULL THEN claimed_amount ELSE 0 END) AS claimed_amount_adjuster_name_is_not_null,
        ROUND(SUM(last_claimed_amount) / NULLIF(SUM(approved_amount), 0), 3) AS last_claimed_amount_to_approved_amount_ratio
    FROM with_windows
    GROUP BY product, adjuster_id, policy_id
    HAVING AVG(approved_amount_max_1_prior) > 15000.0
)
-- Final projection
SELECT
    product,
    policy_id,
    approved_amount_max_1_prior_to_claimed_amount_ra,
    product_list,
    claimed_amount_adjuster_name_is_not_null,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
    SIZE(product_list) AS product_list_count
FROM rollup_base
WHERE approved_amount_max_1_prior_to_claimed_amount_ra > 4.0
