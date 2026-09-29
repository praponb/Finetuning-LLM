select monthly_fee from subscriptions union all select cost as monthly_fee from products where array_contains(tags, 'bestseller');
