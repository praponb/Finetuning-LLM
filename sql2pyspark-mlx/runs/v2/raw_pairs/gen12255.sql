select date_trunc('MONTH', s.opened_date) as opened_month, quarter(s.opened_date) as opened_quarter, count(distinct o.shipping_fee) as unique_stores
from orders o
join stores s on o.store_id = s.id
where s.opened_date < '2023-06-01'
group by date_trunc('MONTH', s.opened_date), quarter(s.opened_date)
