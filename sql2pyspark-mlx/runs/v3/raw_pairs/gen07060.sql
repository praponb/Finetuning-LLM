select s.store_name, ss.product_id, ss.store_id
from stores s
left join store_sales ss on ss.store_id = s.id
where s.sqft between 8713 and 13123 or ss.store_id = 3
order by store_name;
