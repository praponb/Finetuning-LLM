select p.supplier_id, case when p.brand in ('Umbrella', 'Globex') then 'tier_1' else 'other' end as brand_group, round(stddev(ss.revenue), 2) as stddev_revenue
from store_sales ss
inner join products p on ss.product_id = p.id
group by p.supplier_id, case when p.brand in ('Umbrella', 'Globex') then 'tier_1' else 'other' end
