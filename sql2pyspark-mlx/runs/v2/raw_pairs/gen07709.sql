select
    id,
    opened_date,
    store_name,
    ceil(sqft) as sqft_whole
from stores
where city in ('Singapore', 'New York') and sqft between 4592 and 11566
order by sqft_whole desc, id
limit 5
