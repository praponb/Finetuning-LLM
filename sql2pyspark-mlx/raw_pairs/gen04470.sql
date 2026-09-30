WITH base AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        COALESCE(status, 'unassigned') AS status_filled,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM accounts
    WHERE opened_date > TO_DATE('2023-05-05')
      AND status = 'OPEN'
),
enriched AS (
    SELECT
        id,
        account_type,
        status_filled,
        opened_date_month_end,
        SUM(balance) OVER (ORDER BY opened_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        CUME_DIST() OVER (PARTITION BY account_type, customer_id ORDER BY balance DESC NULLS LAST) AS balance_cume_dist
    FROM base
),
remaining_keys AS (
    SELECT account_type
    FROM enriched
    EXCEPT
    SELECT account_type
    FROM accounts
    WHERE opened_date BETWEEN '2023-06-01' AND '2024-11-14'
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.account_type = k.account_type
),
not_flagged AS (
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant NOT IN ('7-Eleven', 'Grab')
        )
)
SELECT
    CASE WHEN running_balance >= 388381 THEN 'LARGE' ELSE 'SMALL' END AS running_balance_tier,
    DATE_TRUNC('QUARTER', opened_date_month_end) AS quarter_start,
    APPROX_PERCENTILE(running_balance, ARRAY(0.25, 0.5, 0.75)) AS running_balance_quartiles,
    PERCENTILE_APPROX(running_balance, ARRAY(0.25, 0.5, 0.75)) AS running_balance_quartiles_2,
    PERCENTILE_APPROX(running_balance, 0.9) AS running_balance_p90
FROM not_flagged
GROUP BY GROUPING SETS ((CASE WHEN running_balance >= 388381 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', opened_date_month_end)), (CASE WHEN running_balance >= 388381 THEN 'LARGE' ELSE 'SMALL' END));
