with numbered as (
    select
        id,
        city,
        opened_date,
        row_number() over (partition by city order by opened_date desc, id) as rnk
    from stores
)
select id, city, opened_date
from numbered
where rnk <= 5
order by city desc;
