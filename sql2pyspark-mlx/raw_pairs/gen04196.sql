WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC NULLS FIRST, id) AS latest_rank
        FROM subscriptions
    ) ranked
    WHERE latest_rank = 1
),
with_dims AS (
    SELECT src.*, d.signup_date
    FROM ranked src
    JOIN customers d
        ON src.customer_id = d.id
    WHERE d.country IN ('US', 'TH', 'DE')
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY start_date NULLS LAST, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
remaining_keys AS (
    SELECT customer_id, id
    FROM latest
    INTERSECT ALL
    SELECT customer_id, id
    FROM subscriptions
    WHERE start_date < '2025-03-09'
),
reconciled AS (
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    c.city,
    e.status,
    COUNT(*) AS cnt,
    AVG(e.monthly_fee) AS avg_monthly_fee
FROM reconciled e
LEFT JOIN customers c ON e.customer_id = c.id
GROUP BY CUBE (c.city, e.status)
ORDER BY status NULLS FIRST;
