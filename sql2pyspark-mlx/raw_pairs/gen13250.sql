select case when merchant in ('Grab', '7-Eleven') then 'group_a' else 'other' end as merchant_group, count(*) as num_transactions, round(avg(amount), 2) as avg_amount
from transactions
where hour(txn_time) between 8 and 16 and account_id = 15 and merchant = 'Grab'
group by 1;
