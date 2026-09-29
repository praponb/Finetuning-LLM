select date_format(s.opened_date, 'yyyy-MM') as opened_month, avg(oi.unit_price) as avg_unit_price, count(oi.quantity) as quantity_count
from order_items oi
left outer join orders o on oi.order_id = o.id
left outer join stores s on s.id = o.store_id
group by date_format(s.opened_date, 'yyyy-MM')
