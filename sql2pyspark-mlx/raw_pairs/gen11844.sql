WITH with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_status
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_status, segment ORDER BY credit_limit DESC NULLS LAST, id) = 1
),
valid AS (
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
        age,
        order_status,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date
    FROM ranked
    WHERE signup_date > DATE '2023-07-15'
      AND segment IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY days_since_signup_date DESC NULLS LAST, id) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
with_latest_customers AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_status_2
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status_2
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    is_active,
    age,
    CASE WHEN country RLIKE '^(JP|UK)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS country_zone
FROM with_latest_customers
ORDER BY age NULLS LAST;
