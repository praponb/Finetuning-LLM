select *
from (
    select
        id,
        segment,
        age,
        email,
        row_number() over (partition by segment order by age, id) as pos
    from customers
) as ranked
where pos <= 1
order by email
