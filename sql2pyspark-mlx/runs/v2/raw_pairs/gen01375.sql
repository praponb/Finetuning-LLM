select quarter(p.launch_date) launch_quarter, count(*) row_count
from inventory i
left join stores s on i.store_id = s.id
inner join products p on p.id = i.product_id
group by quarter(p.launch_date)
order by row_count desc, launch_quarter
limit 3;
