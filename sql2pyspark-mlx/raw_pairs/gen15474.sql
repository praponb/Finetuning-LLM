select *
from (
    select *, row_number() over (partition by customer_id order by balance desc, id) as rn
    from accounts
) as t
where rn <= 1
order by 7 desc
