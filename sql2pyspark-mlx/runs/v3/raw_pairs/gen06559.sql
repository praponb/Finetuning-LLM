select
    case when amount >= 1330 then 'high' when amount >= 440 then 'mid' else 'low' end as amount_bucket,
    sum(amount) as total_amount,
    count(*) as cnt,
    sum(sum(amount)) over (order by case when amount >= 1330 then 'high' when amount >= 440 then 'mid' else 'low' end) as cumulative_amount
from payments
where paid_at >= '2023-10-01'
group by case when amount >= 1330 then 'high' when amount >= 440 then 'mid' else 'low' end
order by total_amount desc, cumulative_amount
