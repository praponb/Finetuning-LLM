SELECT id, end_date, start_date FROM subscriptions WHERE end_date > (SELECT MAX(end_date) FROM subscriptions WHERE end_date BETWEEN '2023-07-24' AND '2023-12-01') ORDER BY end_date DESC;
