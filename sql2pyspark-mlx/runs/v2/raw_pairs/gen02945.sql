select
    category,
    month(launch_date) as launch_month,
    supplier_id,
    round((100 * sum(case when brand = 'Globex' then 1 else 0 end)) / count(*), 1) as pct_globex,
    min(launch_date) as first_launch_date,
    concat_ws(', ', sort_array(collect_set(brand))) as brand_values
from products
group by category, month(launch_date), supplier_id
having pct_globex >= 52;
