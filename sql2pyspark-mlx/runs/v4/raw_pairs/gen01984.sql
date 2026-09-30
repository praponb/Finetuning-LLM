SELECT s.*
FROM customers s
WHERE EXISTS (
        SELECT 1
        FROM subscriptions ch
        WHERE ch.customer_id = s.id
          AND ch.start_date >= s.signup_date
    )
