WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
latest_per_key AS (
    -- Keep the earliest row per adjuster_id, product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, product ORDER BY claim_date, id) AS row_num
        FROM covered
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Summarise per group
    SELECT
        policy_id,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        COUNT(*) FILTER (WHERE claim_type IS NOT NULL) AS n_claim_type_is_not_null,
        SUM(claimed_amount) AS total_claimed_amount,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        ROUND(100 * COUNT(CASE WHEN product = 'LIFE' THEN 1 END) / COUNT(*), 1) AS pct_product_life,
        COUNT(*) AS n_rows
    FROM latest_per_key
    GROUP BY policy_id, DATE_FORMAT(claim_date, 'yyyy-MM')
    HAVING SUM(claimed_amount) > 6745.1
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_id, claim_date_ym, metric, reading
FROM (
    SELECT policy_id, claim_date_ym, total_claimed_amount, pct_product_life
    FROM totals
) src
UNPIVOT (
    reading FOR metric IN (total_claimed_amount, pct_product_life)
) unp
