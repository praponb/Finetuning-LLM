select suppliers.city, count(*) as product_count, sum(case when products.brand = 'Stark' then products.price else 0 end) as stark_price
from products
inner join suppliers on products.supplier_id = suppliers.id
group by suppliers.city
order by 3;
