select *
from (
    select *, row_number() over (partition by customer_id order by started_at desc, id) rn
    from sessions
) ranked
where rn <= 5;
