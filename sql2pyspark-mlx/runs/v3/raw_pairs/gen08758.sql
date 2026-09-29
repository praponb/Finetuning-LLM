select *
from (
    select txn_type, sum(amount) as total_amount
    from transactions
    group by txn_type
) as s
where total_amount < 21000;
