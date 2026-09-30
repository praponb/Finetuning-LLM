select
    category,
    round((100 * sum(case when brand = 'Umbrella' then 1 else 0 end)) / count(*), 1) as pct_umbrella,
    count(distinct stock_qty) as unique_brands,
    count(cost) as cost_count
from products
where product_name like 'Des%' and price < 830 and category = 'Fashion'
group by category
order by unique_brands desc, pct_umbrella;
