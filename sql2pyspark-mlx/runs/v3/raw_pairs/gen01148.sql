select
    case when txn_type in ('FEE', 'REFUND') then 'core' else 'other' end as txn_type_group,
    txn_type,
    merchant,
    round(avg(amount), 2) as avg_amount,
    round((100 * sum(case when merchant = 'Amazon' then 1 else 0 end)) / count(*), 1) as pct_amazon
from transactions
group by 1, 2, 3
having pct_amazon > 57
order by txn_type_group asc
