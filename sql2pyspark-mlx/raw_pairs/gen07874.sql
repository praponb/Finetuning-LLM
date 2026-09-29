select
    category,
    case when cost >= 550 then 'high' when cost >= 120 then 'mid' else 'low' end as cost_bucket,
    count(distinct price) as unique_categorys,
    sum(case when brand = 'Acme' then stock_qty else 0 end) as acme_stock_qty
from products
where launch_date > '2023-03-31' and supplier_id = 38
group by 1, 2
order by 4 desc, unique_categorys
