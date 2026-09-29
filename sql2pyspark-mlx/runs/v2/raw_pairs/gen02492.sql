select round((100 * sum(case when status = 'CANCELLED' then 1 else 0 end)) / count(*), 1) as pct_cancelled, max(shipping_fee) - min(shipping_fee) as shipping_fee_range
from orders
where store_id = 7
order by pct_cancelled asc;
