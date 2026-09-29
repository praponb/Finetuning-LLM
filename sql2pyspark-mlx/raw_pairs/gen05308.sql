select plan, status from subscriptions intersect select plan, status from subscriptions where customer_id = 38;
