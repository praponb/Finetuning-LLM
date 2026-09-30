select
    id,
    store_name,
    sqft,
    round(sqft / sum(sqft) over (partition by city), 4) as sqft_share,
    row_number() over (partition by region order by sqft, id) as seq_no,
    ntile(4) over (order by sqft desc, id) as sqft_bucket
from stores
where sqft between 6079 and 8645
