WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-04-10')
      AND plan = 'enterprise'
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.start_date,
        s.status,
        s.start_date_month_end,
        d.email
    FROM valid s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY start_date, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM latest
    INTERSECT
    SELECT id, status
    FROM subscriptions
    WHERE status IN ('PAUSED', 'CANCELLED', 'EXPIRED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
)
SELECT
    s.id,
    s.customer_id,
    s.start_date_month_end,
    s.email,
    (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_amount
FROM kept_rows s
ORDER BY customer_id DESC NULLS LAST
