select case when year(order_date) < 2024 then 'before_2024' when year(order_date) = 2024 then '2024' else 'after_2024' end as order_cohort, channel, sum(case when channel = 'store' then 1 else 0 end) as store_count
from orders
group by order_cohort, channel
having avg(amount) < 970
order by channel desc, store_count, order_cohort
limit 50
