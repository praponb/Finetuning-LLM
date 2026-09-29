WITH enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.status,
        b.signup_date,
        b.country,
        b.city
    FROM accounts src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.credit_limit < 5000
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC NULLS LAST, id) = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.status,
        e.country,
        e.city,
        d.city AS customer_city
    FROM ranked e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < '2025-04-01'
        )
)
-- Grouped metrics
SELECT
    status,
    country,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT(*) AS row_count,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY COUNT(*) DESC) AS row_count_percent_rank
FROM not_flagged
GROUP BY status, country
HAVING row_count > 1;
