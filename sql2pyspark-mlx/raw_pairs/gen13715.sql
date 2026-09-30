select * from (select account_id, sum(amount) as total_amount, round(avg(amount), 2) as avg_amount from transactions group by account_id) as sub where total_amount >= 13000 order by total_amount desc
