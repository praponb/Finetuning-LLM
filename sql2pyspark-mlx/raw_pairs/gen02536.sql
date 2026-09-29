SELECT amount FROM transactions WHERE account_id = 2 INTERSECT SELECT monthly_fee FROM subscriptions WHERE monthly_fee BETWEEN 110 AND 160;
