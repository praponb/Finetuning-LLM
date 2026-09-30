select *
from (
    select *, dense_rank() over (partition by order_id order by shipped_date desc) as rnk
    from shipments
    where year(shipped_date) = 2023
) as ranked
where rnk <= 3
