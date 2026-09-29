WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE claimed_amount <= 33690.0
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        YEAR(reported_at) AS reported_at_year,
        status,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        MIN(approved_amount) AS min_approved_amount,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM prepared_claims
    GROUP BY DATE_TRUNC('MONTH', claim_date), YEAR(reported_at), status
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(median_claimed_amount) OVER (PARTITION BY reported_at_year, status) AS reported_at_year_status_min_median_claimed_amoun,
        COUNT(*) OVER (PARTITION BY reported_at_year, status) AS rows_in_reported_at_year_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_rows, reported_at_year ORDER BY median_claimed_amount), 3) AS median_claimed_amount_percent_rank,
        ROUND(AVG(median_claimed_amount) OVER (PARTITION BY YEAR(claim_date_month), status ORDER BY avg_claimed_amount ASC NULLS LAST, claim_date_month, reported_at_year, status ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS median_claimed_amount_avg_3,
        DENSE_RANK() OVER (PARTITION BY YEAR(claim_date_month), status ORDER BY avg_claimed_amount ASC NULLS LAST, claim_date_month, reported_at_year, status) AS avg_claimed_amount_dense_rank,
        ROUND(median_claimed_amount - AVG(median_claimed_amount) OVER (PARTITION BY reported_at_year, status), 2) AS median_claimed_amount_vs_reported_at_year_status
    FROM rollup_base
)
SELECT
    claim_date_month,
    status,
    status_list,
    min_approved_amount,
    median_claimed_amount,
    n_rows,
    claim_type_list,
    reported_at_year_status_min_median_claimed_amoun,
    median_claimed_amount_avg_3,
    avg_claimed_amount_dense_rank,
    median_claimed_amount_vs_reported_at_year_status,
    COALESCE(min_approved_amount, 0.0) AS min_approved_amount_filled
FROM ranked
ORDER BY median_claimed_amount DESC
