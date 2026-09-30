with numbered as (
    select
        id,
        txn_type,
        amount,
        row_number() over (partition by txn_type order by amount desc, id) as rn
    from transactions
)
select *
from numbered
where rn = 1;
