select *
from (
    select *, row_number() over (partition by region order by opened_date desc, id) as rn
    from stores
    where store_name like '%Mall%'
) as r
where rn <= 2;
