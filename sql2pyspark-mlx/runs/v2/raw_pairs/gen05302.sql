select *
from (
    select *, row_number() over (partition by country order by rating, id) rn
    from suppliers
) ranked
where rn <= 3
order by country;
