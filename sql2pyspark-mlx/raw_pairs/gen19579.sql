WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label,
        COALESCE(segment, 'N/A') AS segment_filled
    FROM customers
    WHERE segment IS DISTINCT FROM 'SMB'
      AND segment = 'Corporate'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        segment_filled,
        COALESCE(email, 'Unknown') AS email_filled
    FROM valid
    WHERE signup_date >= DATE '2023-09-07'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date < s.signup_date
        )
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_amount
    FROM kept s
    JOIN LATERAL (
            SELECT c.amount AS order_amount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_customers AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_status,
        top1.order_order_date
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status, c.order_date AS order_order_date
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    email,
    credit_limit,
    age,
    segment_filled,
    email_filled
FROM with_last_customers;
