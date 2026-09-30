with transactions_by_account as (
    select account_id, sum(case when merchant = 'Lazada' then amount else 0 end) as lazada_amount, round((100 * sum(case when merchant = 'Grab' then 1 else 0 end)) / count(*), 1) as pct_grab
    from transactions
    where txn_type in ('CREDIT', 'FEE', 'REFUND')
    group by account_id
)
select a.account_type, tba.lazada_amount, tba.pct_grab
from accounts a
join transactions_by_account tba on a.id = tba.account_id
where tba.lazada_amount > 29
order by tba.pct_grab desc;
