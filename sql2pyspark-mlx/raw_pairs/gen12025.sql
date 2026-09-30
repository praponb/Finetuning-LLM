WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE opened_date > TO_DATE('2023-09-28')
      AND status = 'OPEN'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    customer_id,
    APPROX_PERCENTILE(balance, 0.95) AS balance_p95,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles_2,
    MAX(balance) AS max_balance,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(balance) AS balance_stddev,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', opened_date) ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_rank
FROM base
GROUP BY DATE_TRUNC('QUARTER', opened_date), customer_id
HAVING balance_p95 < 1000000
ORDER BY balance_stddev DESC NULLS FIRST;
