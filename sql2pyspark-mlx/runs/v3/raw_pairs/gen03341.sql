select category, sum(price) as total_price, count(*) as cnt, rank() over (order by sum(price) desc) as rnk from products where supplier_id is null group by category order by 2 desc, cnt desc;
