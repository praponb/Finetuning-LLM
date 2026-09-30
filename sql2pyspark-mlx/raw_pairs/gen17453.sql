WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(account_type)) AS account_type_title,
        SUBSTRING(account_type, 1, 4) AS account_type_prefix
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-03-23')
),
enriched AS (
    SELECT
        src.*,
        d.age,
        d.credit_limit,
        d.signup_date
    FROM base src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
base_accounts AS (
    SELECT *, UPPER(status) AS status_upper
    FROM enriched
    WHERE customer_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY signup_date DESC NULLS LAST, id) AS rn
        FROM base_accounts
    ) t
    WHERE rn = 1
),
metrics AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        account_type_title,
        age,
        signup_date,
        SUM(age) OVER (PARTITION BY customer_id) AS customer_id_sum_age,
        AVG(credit_limit) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_3_prior
    FROM latest
    WHERE balance < 45000
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    status,
    age,
    signup_date,
    customer_id_sum_age,
    credit_limit_avg_3_prior
FROM metrics
ORDER BY status NULLS LAST;
