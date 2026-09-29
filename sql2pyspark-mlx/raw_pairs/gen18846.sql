select t.id, t.merchant, a.balance, a.customer_id, c.city from transactions t full outer join accounts a on t.account_id = a.id join customers c on c.id = a.customer_id order by balance
