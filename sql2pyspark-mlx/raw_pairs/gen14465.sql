SELECT
    id,
    status,
    opened_date,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY opened_date) AS opened_date_pct_rank
FROM accounts
