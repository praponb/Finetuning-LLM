WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        SUBSTRING(segment, 1, 2) AS segment_prefix
    FROM customers
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.account_status
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(credit_limit, age) OVER (PARTITION BY segment) AS corr_credit_limit_age,
        LAG(credit_limit, 1) OVER (PARTITION BY city, country ORDER BY signup_date, id) AS prev_credit_limit,
        NTILE(4) OVER (PARTITION BY account_status, country ORDER BY age DESC, id) AS age_bucket,
        NTILE(4) OVER (ORDER BY age, id) AS age_bucket_2
    FROM kept
),
enriched_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY segment ORDER BY age_bucket_2 DESC) AS age_bucket_2_rank,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM enriched
    WINDOW w_part AS (PARTITION BY city ORDER BY corr_credit_limit_age DESC, id)
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    credit_limit,
    age,
    segment_prefix,
    corr_credit_limit_age,
    CASE WHEN segment RLIKE '^(Consumer|SMB)$' THEN 'CORE' ELSE 'PRIORITY' END AS segment_zone
FROM enriched_customers
ORDER BY age NULLS FIRST
