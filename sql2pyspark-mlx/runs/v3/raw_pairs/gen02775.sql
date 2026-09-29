select date_format(s.opened_date, 'yyyy-MM') as opened_month, case when s.sqft >= 12355 then 'high' when s.sqft >= 2940 then 'mid' else 'low' end as sqft_bucket, avg(ss.revenue) as avg_revenue
from store_sales ss
left join stores s on ss.store_id = s.id
where s.store_name like 'Ol%'
group by date_format(s.opened_date, 'yyyy-MM'), case when s.sqft >= 12355 then 'high' when s.sqft >= 2940 then 'mid' else 'low' end
order by sqft_bucket
