select *
from (
    select
        year(txn_time) as txn_year,
        case when amount >= 4000 then 'high' when amount >= 800 then 'mid' else 'low' end as amount_bucket,
        sum(case when txn_type = 'DEBIT' then 1 else 0 end) as debit_count,
        count(distinct amount) as unique_merchants
    from transactions
    group by year(txn_time), case when amount >= 4000 then 'high' when amount >= 800 then 'mid' else 'low' end
) as x
where debit_count > 65
order by debit_count desc, txn_year, amount_bucket
limit 50
