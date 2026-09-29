SELECT amount FROM payments UNION SELECT monthly_fee FROM subscriptions ORDER BY amount;
