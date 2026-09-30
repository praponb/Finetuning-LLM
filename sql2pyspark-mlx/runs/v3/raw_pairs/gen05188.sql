select *
from (
    select region, min(opened_date) as min_opened_date, count(sqft) as sqft_count
    from stores
    group by region
) as sub
where sqft_count < 5
order by sqft_count;
