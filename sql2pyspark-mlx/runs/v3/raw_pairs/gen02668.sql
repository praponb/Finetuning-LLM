select category, unique_brands
from (
    select category, count(distinct price) as unique_brands
    from products
    group by category
) as x
where unique_brands > 9
order by category, unique_brands desc;
