WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY signup_date DESC NULLS FIRST) AS row_num
    FROM customers
    QUALIFY row_num = 1
),
with_latest AS (
    SELECT s.*, latest.status
    FROM ranked s
    JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
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
        row_num,
        status,
        REGEXP_REPLACE(email, ' ', '_') AS email_slug,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(segment, 'NONE') AS segment_clean
    FROM with_latest
    WHERE signup_date > DATE '2023-06-09'
      AND city IS NOT DISTINCT FROM 'Singapore'
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND MONTH(ch.opened_date) IN (3, 5, 9)
        )
)
SELECT *
FROM (
    SELECT
        city,
        status,
        credit_limit
    FROM not_flagged
) src
PIVOT (
    AVG(credit_limit)
    FOR status IN ('SHIPPED' AS shipped, 'COMPLETED' AS completed)
);
