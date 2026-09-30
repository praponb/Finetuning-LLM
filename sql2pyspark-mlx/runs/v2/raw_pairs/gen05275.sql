select
    region,
    opened_date,
    store_name,
    coalesce(store_name, 'N/A') as store_name_filled
from stores
where store_name not like '%Mal%';
