select account_id, sum(case when txn_type = 'FEE' then 1 else 0 end) as fee_count
from transactions
where account_id in (17, 19, 33)
group by account_id
having sum(case when txn_type = 'FEE' then 1 else 0 end) < 77
order by account_id desc
