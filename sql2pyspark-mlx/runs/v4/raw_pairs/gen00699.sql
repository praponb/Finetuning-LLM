select date_trunc('MONTH', s.opened_date) as opened_month, count(distinct i.quantity) as unique_stores
from inventory i
left join stores s on i.store_id = s.id
full outer join products p on i.product_id = p.id
group by date_trunc('MONTH', s.opened_date)
order by unique_stores desc
