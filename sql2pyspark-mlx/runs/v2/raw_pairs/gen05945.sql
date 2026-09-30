select t.merchant, a.customer_id from transactions t full outer join accounts a on t.account_id = a.id where t.account_id = 37 or a.status = 'OPEN';
