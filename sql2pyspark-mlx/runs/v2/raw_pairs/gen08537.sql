with numbered as (
    select
        id,
        location,
        budget,
        rank() over (partition by location order by budget desc) as rn
    from departments
)
select *
from numbered
where rn <= 5
order by rn desc, id desc
