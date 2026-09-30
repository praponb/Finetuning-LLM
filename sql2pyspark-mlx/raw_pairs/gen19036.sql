with valid as (
    -- Filter the raw rows and derive helper columns
    select *, substring(status, 1, 2) as status_prefix
    from shipments
    where shipped_date >= to_date('2023-06-07')
)
-- QUALIFY-style window filter
select *
from (
    select *, row_number() over (partition by carrier order by shipped_date, id) as latest_rank
    from valid
) t
where latest_rank = 1
