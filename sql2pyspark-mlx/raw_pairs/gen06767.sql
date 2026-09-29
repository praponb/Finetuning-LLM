select
    id,
    region,
    opened_date,
    coalesce(store_name, 'unassigned') as store_name_filled
from stores
order by region, store_name_filled;
