select case when method in ('paypal', 'cash') then 'tier_1' else 'other' end as method_group, round(stddev(amount), 2) as stddev_amount
from payments
where (paid_at < '2024-01-03' and amount between 1230 and 1430) or order_id is not null
group by method_group
having stddev_amount > 62
order by method_group asc;
