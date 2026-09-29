WITH matched AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
summary AS (
    -- Summarise per group
    SELECT
        policy_id,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        COUNT(*) AS n_rows,
        COUNT(*) FILTER (WHERE product IS NOT NULL) AS n_product_is_not_null
    FROM matched
    GROUP BY 1, 2
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', claim_date_month) AS days_since_claim_date_month,
        FLOOR(n_product_is_not_null / 2) * 2 AS n_product_is_not_null_bucket,
        DATE_FORMAT(claim_date_month, 'MMM yyyy') AS claim_date_month_label
    FROM summary
    WHERE n_product_is_not_null > 2
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_id, claim_date_month, metric_name, reading
FROM (
    SELECT policy_id, claim_date_month, distinct_adjuster_ids, n_rows, n_product_is_not_null
    FROM tagged
) src
UNPIVOT (
    reading FOR metric_name IN (distinct_adjuster_ids AS distinct_adjuster_ids_v, n_rows AS m_n_rows, n_product_is_not_null AS m_n_product_is_not_null)
) unp
