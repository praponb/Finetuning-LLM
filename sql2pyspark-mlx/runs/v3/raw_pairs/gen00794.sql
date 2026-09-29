select end_date, 'subscription' source from subscriptions union select launch_date, 'product' source from products where price between 490 and 930;
