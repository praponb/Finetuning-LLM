WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS rn FROM subscriptions) SELECT id, customer_id, start_date, plan FROM numbered WHERE rn <= 1;
