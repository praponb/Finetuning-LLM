SELECT
    id,
    customer_id,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS end_date_row
FROM subscriptions;
