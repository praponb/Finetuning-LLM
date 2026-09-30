select *
from (
    select method, count(amount) as amount_count, round((100 * sum(case when method = 'promptpay' then 1 else 0 end)) / count(*), 1) as pct_promptpay
    from payments
    group by method
) as agg
where amount_count < 8
order by pct_promptpay desc, amount_count desc
