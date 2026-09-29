WITH cleaned AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        CASE WHEN segment IN ('Consumer', 'Corporate') THEN 'Tier 1' ELSE 'Other' END segment_group
    FROM customers
    WHERE signup_date > DATE '2023-08-29'
      AND city IS NOT NULL
)
SELECT s.*
FROM cleaned s
WHERE EXISTS (
        SELECT 1
        FROM orders ch
        WHERE ch.customer_id = s.id
          AND ch.customer_id = 27
    );
