select *
from (
    select txn_type, case when amount >= 3000 then 'high' when amount >= 800 then 'mid' else 'low' end as amount_bucket, avg(amount) as avg_amount
    from transactions
    where merchant not in ('Grab', '7-Eleven')
    group by txn_type, case when amount >= 3000 then 'high' when amount >= 800 then 'mid' else 'low' end
) as sub
where avg_amount < 3300
order by txn_type desc
