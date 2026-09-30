select
    *,
    sum(sqft) over (partition by region order by opened_date, id rows between unbounded preceding and current row) as running_sqft,
    round(sqft / sum(sqft) over (partition by year(opened_date), city), 4) as sqft_share
from stores
where sqft between 6664 and 9394;
