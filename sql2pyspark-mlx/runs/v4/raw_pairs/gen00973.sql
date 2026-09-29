with top_departments as (
    select *, row_number() over (partition by location order by budget desc, id) as rnk
    from departments
)
select *
from top_departments
where rnk <= 1;
