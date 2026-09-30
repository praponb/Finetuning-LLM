select
    id,
    region,
    opened_date,
    store_name
from stores
where opened_date between '2023-03-22' and '2024-03-03' and opened_date < '2024-04-06' and opened_date >= '2024-05-27'
order by id
limit 3;
