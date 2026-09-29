select
    customer_id,
    account_type,
    count(distinct account_type) as distinct_account_types,
    sum(case when account_type = 'LOAN' then balance else 0 end) as loan_balance,
    size(collect_set(status)) as num_status,
    grouping_id() as gid
from accounts
group by grouping sets ((customer_id, account_type), (customer_id))
having loan_balance > 0
order by customer_id
