with with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(sqft) over (partition by region) as region_sum_sqft,
        row_number() over (partition by region order by opened_date desc, id) as seq_no,
        ntile(3) over (order by sqft, id) as sqft_bucket,
        ntile(4) over (partition by region, city order by sqft desc, id) as sqft_bucket_2
    from stores
    where city = 'Tokyo'
)
select
    id,
    city,
    opened_date,
    seq_no,
    sqft_bucket,
    sqft_bucket_2
from with_windows
