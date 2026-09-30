select
    id,
    account_id,
    merchant,
    is_flagged
from (
    select *, rank() over (partition by txn_type order by amount desc) as latest_rank
    from transactions
) t
where latest_rank = 1;
