WITH filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        INITCAP(TRIM(status)) AS status_title,
        WEEKOFYEAR(opened_date) AS opened_date_weekofyear
    FROM accounts
    WHERE status IS DISTINCT FROM 'OPEN'
)
SELECT
    CASE WHEN opened_date_weekofyear >= 87 THEN 'HIGH' ELSE 'LOW' END AS opened_date_weekofyear_tier,
    status,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    COUNT_IF(status = 'CLOSED') AS closed_events,
    APPROX_PERCENTILE(opened_date_weekofyear, 0.5) AS opened_date_weekofyear_p50,
    STDDEV_SAMP(balance) AS balance_stddev,
    RANK() OVER (PARTITION BY CASE WHEN opened_date_weekofyear >= 87 THEN 'HIGH' ELSE 'LOW' END ORDER BY APPROX_PERCENTILE(opened_date_weekofyear, 0.5) DESC) AS opened_date_weekofyear_p50_rank
FROM filtered
GROUP BY CASE WHEN opened_date_weekofyear >= 87 THEN 'HIGH' ELSE 'LOW' END, status
HAVING balance_stddev > 0
ORDER BY closed_events DESC NULLS FIRST;
