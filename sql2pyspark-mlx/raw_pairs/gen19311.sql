SELECT c.id, c.age, t.first_opened_date
FROM customers c
INNER JOIN (SELECT customer_id, MIN(opened_date) AS first_opened_date
    FROM accounts
    WHERE balance >= 27000
    GROUP BY customer_id) t ON c.id = t.customer_id;
