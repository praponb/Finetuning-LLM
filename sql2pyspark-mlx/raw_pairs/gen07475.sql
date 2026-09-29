select
    brand,
    category,
    case when year(launch_date) < 2024 then 'before_2024' when year(launch_date) = 2024 then '2024' else 'after_2024' end as launch_cohort,
    max(stock_qty) - min(stock_qty) as stock_qty_range,
    sort_array(collect_set(brand)) as brand_list,
    concat_ws(', ', sort_array(collect_set(category))) as category_values
from products
group by brand, category, case when year(launch_date) < 2024 then 'before_2024' when year(launch_date) = 2024 then '2024' else 'after_2024' end
order by launch_cohort, brand;
