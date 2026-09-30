select *
from (
    select *, row_number() over (partition by country order by rating desc, id) as rnk
    from suppliers
) as r
where rnk = 1
order by country;
