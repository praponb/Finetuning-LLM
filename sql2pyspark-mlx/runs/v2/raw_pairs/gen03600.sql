select
    brand,
    supplier_id,
    count(distinct cost) as unique_suppliers,
    max(launch_date) as last_launch_date
from products
where size(tags) >= 1
group by rollup(brand, supplier_id)
order by last_launch_date, 1 desc
