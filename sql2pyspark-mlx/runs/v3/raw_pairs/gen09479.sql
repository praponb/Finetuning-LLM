select
    id,
    city,
    rating,
    supplier_name,
    sum(rating) over (partition by city) as city_sum_rating,
    last_value(supplier_name) over (order by rating desc, id rows between unbounded preceding and unbounded following) as last_supplier_name
from suppliers
where country in ('US', 'JP', 'UK');
