select id, customer_id, opened_date, balance, first_value(balance) over (partition by customer_id order by opened_date, id) as first_balance from accounts where balance between 49000 and 56000
