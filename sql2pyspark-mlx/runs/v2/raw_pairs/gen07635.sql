select
    a.id,
    a.status,
    count(t.id) as transaction_count,
    sum(case when t.merchant = 'Starbucks' then t.amount else 0 end) as starbucks_amount,
    round((100 * sum(case when t.merchant = '7-Eleven' then 1 else 0 end)) / count(*), 1) as pct_7_eleven
from accounts a
left join transactions t on t.account_id = a.id
where t.is_flagged
group by a.id, a.status;
