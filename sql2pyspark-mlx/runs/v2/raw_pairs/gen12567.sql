select *
from (
    select *, rank() over (partition by city order by rating desc) as row_num
    from suppliers
    where length(supplier_name) >= 8
) as r
where row_num <= 3
order by city
