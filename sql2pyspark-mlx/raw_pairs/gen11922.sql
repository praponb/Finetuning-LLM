with ranked as (
    select
        id,
        region,
        opened_date,
        sqft,
        row_number() over (partition by region order by opened_date desc, id) as rnk
    from stores
    where city = 'Singapore'
)
select
    id,
    region,
    opened_date,
    sqft
from ranked
where rnk <= 2
