select quarter(s.opened_date) as opened_quarter, max(ss.sale_date) as last_sale_date, max(ss.units) as max_units
from store_sales ss
join products p on p.id = ss.product_id
left join stores s on ss.store_id = s.id
group by quarter(s.opened_date)
order by max_units desc, opened_quarter
limit 50;
