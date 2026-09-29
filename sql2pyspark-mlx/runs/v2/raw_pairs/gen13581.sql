select *
from (
    select
        id,
        city,
        opened_date,
        store_name,
        dense_rank() over (partition by city order by opened_date desc) as rnk
    from stores
) as ranked
where rnk = 1;
