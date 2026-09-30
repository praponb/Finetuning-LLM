select quarter(s.opened_date) as opened_quarter, max(ss.revenue) - min(ss.revenue) as revenue_range, max(ss.sale_date) as latest_sale_date
from store_sales ss
inner join stores s on s.id = ss.store_id
left join products p on p.id = ss.product_id
group by quarter(s.opened_date)
