WITH in_window AS (
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
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        policy_id,
        status,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount,
        MAX(claim_date) AS last_claim_date,
        COUNT(*) FILTER (WHERE product IS NOT NULL) AS n_product_is_not_null
    FROM in_window
    GROUP BY DATE_TRUNC('MONTH', claim_date), policy_id, status
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(row_count, n_product_is_not_null) AS least_row_count_n_product_is_not_null,
        DAYOFWEEK(last_claim_date) AS last_claim_date_dayofweek
    FROM per_group
    WHERE n_product_is_not_null < 8
),
labeled AS (
    -- Add calculated columns
    SELECT
        claim_date_month,
        policy_id,
        status,
        n_product_is_not_null,
        least_row_count_n_product_is_not_null,
        last_claim_date_dayofweek,
        FLOOR(n_product_is_not_null / 2) * 2 AS n_product_is_not_null_bucket,
        row_count + median_claimed_amount AS row_count_plus_median_claimed_amount
    FROM enriched
)
SELECT *
FROM (
    SELECT least_row_count_n_product_is_not_null, status, row_count_plus_median_claimed_amount
    FROM labeled
) src
PIVOT (
    MAX(row_count_plus_median_claimed_amount)
    FOR status IN ('IN_REVIEW', 'APPROVED', 'OPEN')
)
