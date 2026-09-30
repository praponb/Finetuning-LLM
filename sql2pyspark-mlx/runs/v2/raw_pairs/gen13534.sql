select account_id, refund_amount
from (
    select account_id, sum(case when txn_type = 'REFUND' then amount else 0 end) as refund_amount
    from transactions
    group by account_id
) as t
where refund_amount > 74;
