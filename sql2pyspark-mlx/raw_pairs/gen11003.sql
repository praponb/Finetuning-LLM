WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(signup_date) AS signup_date_month_end
    FROM customers
    WHERE segment IS NOT NULL
      AND signup_date <= '2023-03-26'
)
-- Latest matching session per row (LATERAL ... LIMIT 1)
SELECT
    s.id,
    s.name,
    s.country,
    s.city,
    s.credit_limit,
    s.age,
    s.signup_date_month_end,
    top1.session_country
FROM filtered s
LEFT JOIN LATERAL (
        SELECT c.country AS session_country
        FROM sessions c
        WHERE c.customer_id = s.id
        ORDER BY c.started_at DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
