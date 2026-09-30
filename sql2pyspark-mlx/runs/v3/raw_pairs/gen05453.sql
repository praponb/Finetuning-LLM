select id, customer_id, coalesce(balance, 0) as balance_filled from accounts where balance is not null and opened_date between '2023-05-01' and '2024-08-01' order by id desc;
