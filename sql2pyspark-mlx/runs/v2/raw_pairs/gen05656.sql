select status, sum(case when status = 'OPEN' then 1 else 0 end) as open_count from accounts where balance between 23000 and 67000 and customer_id is null group by 1
