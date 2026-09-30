select order_date from orders union select end_date from subscriptions where month(start_date) in (1, 8, 9)
