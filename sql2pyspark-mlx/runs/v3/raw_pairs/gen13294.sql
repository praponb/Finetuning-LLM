select *
from (
    select *, row_number() over (partition by city order by rating desc, id) as rn
    from suppliers
) as t
where rn <= 2
order by rn desc;
