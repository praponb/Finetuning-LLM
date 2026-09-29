select
    id,
    sale_date,
    store_id,
    round(units / revenue, 2) as revenue_per_unit
from store_sales
where units >= 38 or sale_date between '2023-07-05' and '2024-01-09';
