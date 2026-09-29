select case when products.category in ('Grocery', 'Fashion') then 'group_a' else 'other' end category_group, sum(store_sales.revenue) total_revenue
from store_sales
inner join products on store_sales.product_id = products.id
left outer join suppliers on products.supplier_id = suppliers.id
group by case when products.category in ('Grocery', 'Fashion') then 'group_a' else 'other' end;
