select *
from (
    select *, row_number() over (partition by txn_type order by txn_time desc, id) rn
    from transactions
) t
where rn <= 5
order by rn desc
