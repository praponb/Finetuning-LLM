select status from orders except select status from subscriptions where end_date > '2023-09-05';
