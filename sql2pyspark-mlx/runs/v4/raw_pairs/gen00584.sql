select *
from (
    select *, row_number() over (partition by city order by rating desc nulls first, id) as row_num
    from suppliers
) ranked
where row_num = 1;
