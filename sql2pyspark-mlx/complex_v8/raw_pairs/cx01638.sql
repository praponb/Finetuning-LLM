WITH joined_ranges AS (
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
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        product,
        adjuster_id,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        MAX(claim_date) AS last_claim_date,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        MIN(approved_amount) AS min_approved_amount,
        COUNT(DISTINCT product) AS distinct_products
    FROM joined_ranges
    GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM'), product, adjuster_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        claim_date_span_days - LAG(claim_date_span_days) OVER w_last_claim_date_month_by_cla AS claim_date_span_days_change,
        SUM(avg_claimed_amount) OVER (PARTITION BY distinct_products) AS distinct_products_sum_avg_claimed_amount
    FROM grouped
    WINDOW w_last_claim_date_month_by_cla AS (PARTITION BY MONTH(last_claim_date) ORDER BY claim_date_span_days ASC, claim_date_ym, product, adjuster_id)
)
SELECT
    claim_date_ym,
    product,
    adjuster_id,
    last_claim_date,
    claim_date_span_days,
    min_approved_amount,
    distinct_products,
    claim_date_span_days_change,
    LOWER(REGEXP_REPLACE(TRIM(claim_date_ym), ' ', '_')) AS claim_date_ym_slug
FROM ranked
WHERE claim_date_span_days >= 140
ORDER BY claim_date_ym_slug DESC
