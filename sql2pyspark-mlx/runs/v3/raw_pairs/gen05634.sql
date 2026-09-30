SELECT monthly_fee FROM subscriptions EXCEPT SELECT amount FROM transactions ORDER BY monthly_fee
