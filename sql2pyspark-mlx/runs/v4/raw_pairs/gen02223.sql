with matched_keys as (
    select city, id
    from stores
    except all
    select city, id
    from stores
    where city = 'Berlin'
),
reconciled as (
    select s.*
    from stores s
    inner join matched_keys k on s.city = k.city and s.id = k.id
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft
from reconciled
