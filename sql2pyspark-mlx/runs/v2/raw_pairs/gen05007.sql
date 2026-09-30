select accounts.id, accounts.balance from accounts where accounts.customer_id in (select id from customers where credit_limit between 5800 and 7400)
