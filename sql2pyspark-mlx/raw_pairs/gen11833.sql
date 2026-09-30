select p.segment, p.city, count(*) as num_rows, min(s.balance) as min_balance from accounts s inner join customers p on s.customer_id = p.id group by rollup (p.segment, p.city);
