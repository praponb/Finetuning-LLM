select date_trunc('MONTH', s.opened_date) as opened_month, round(stddev(i.quantity), 2) as stddev_quantity, max(i.quantity) as max_quantity
from inventory i
inner join stores s on s.id = i.store_id
group by date_trunc('MONTH', s.opened_date)
having max_quantity > 220
order by max_quantity desc, opened_month desc
limit 10
