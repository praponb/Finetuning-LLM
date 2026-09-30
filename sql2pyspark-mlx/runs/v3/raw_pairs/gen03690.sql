select sum(case when category = 'Books' then cost else 0 end) as books_cost, sum(price) as total_price from products order by 1 desc;
