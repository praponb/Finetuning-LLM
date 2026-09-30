select accounts.id, accounts.customer_id, accounts.balance, accounts.opened_date, customers.name from accounts left join customers on accounts.customer_id = customers.id order by opened_date
