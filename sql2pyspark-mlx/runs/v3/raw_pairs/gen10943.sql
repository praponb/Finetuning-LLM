select monthly_fee from subscriptions where plan != 'standard' intersect select shipping_fee as monthly_fee from orders;
