select id, country, rating
from (
    select *, dense_rank() over (partition by country order by rating desc) as rnk
    from suppliers
    where supplier_name like 'Li%'
) as t
where rnk <= 3
order by country desc, id desc
