WITH covered AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        policy_id,
        product,
        COUNT(*) FILTER (WHERE product <> 'HOME') AS n_product_home,
        SUM(claimed_amount) FILTER (WHERE product NOT IN ('TRAVEL', 'HEALTH')) AS claimed_amount_product_not_in_travel_health,
        COUNT(*) FILTER (WHERE product IN ('HOME', 'HEALTH')) AS n_product_in_home_health
    FROM covered
    GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM'), policy_id, product
    HAVING MAX(approved_amount) > 0.0
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount,
        (SELECT COUNT(CASE WHEN c.claim_type IS NOT NULL THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM stats s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER w_all_by_claim_claim_type_coun AS claim_claim_type_count_quartile,
        NTH_VALUE(avg_claim_approved_amount, 2) OVER (ORDER BY claim_claim_type_count, claim_date_ym, policy_id, product ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_claim_approved_amount
    FROM summarized_rows
    WINDOW w_all_by_claim_claim_type_coun AS (ORDER BY claim_claim_type_count, claim_date_ym, policy_id, product)
)
-- Final projection
SELECT
    claim_date_ym,
    policy_id,
    n_product_home,
    n_product_in_home_health,
    avg_claim_approved_amount,
    claim_claim_type_count,
    claim_count,
    second_avg_claim_approved_amount,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
    CONCAT_WS('-', claim_date_ym, product) AS claim_date_ym_product_label
FROM sequenced
WHERE second_avg_claim_approved_amount > 17500.0
