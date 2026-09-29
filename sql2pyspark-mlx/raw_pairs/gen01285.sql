select
    supplier_id,
    case when cost >= 560 then 'high' when cost >= 270 then 'mid' else 'low' end as cost_bucket,
    sum(case when category = 'Books' then price else 0 end) as books_price,
    max(price) - min(price) as price_range
from products
where product_name like '%Yoga%'
group by supplier_id, cost_bucket
having count(*) >= 5
order by 4 desc;
