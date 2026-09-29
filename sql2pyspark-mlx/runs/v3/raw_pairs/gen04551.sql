select
    id,
    opened_date,
    city,
    case when city = 'New York' then 1 else 0 end is_new_york
from stores
where opened_date <= '2024-01-01' or store_name like '%Kiosk%'
order by is_new_york desc;
