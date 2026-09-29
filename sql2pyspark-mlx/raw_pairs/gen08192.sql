select id, customer_id from accounts where balance < 63000 and (account_type = 'SAVINGS' or opened_date between '2023-07-01' and '2024-05-01') order by id
