WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date BETWEEN '2023-09-01' AND '2024-10-01'
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) / 7 AS weeks_since_signup_date,
        CASE WHEN city IN ('Singapore', 'London') THEN 'Core' ELSE 'Other' END AS city_group
    FROM kept
    WHERE signup_date > DATE '2023-05-04'
      AND segment IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        country,
        is_active,
        age,
        weeks_since_signup_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) x
    WHERE rank_in_group = 1
),
deduped_customers AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rank_in_group
        FROM deduped
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    name,
    is_active,
    weeks_since_signup_date
FROM deduped_customers
ORDER BY id;
