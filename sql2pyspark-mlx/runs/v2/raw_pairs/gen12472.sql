select
    id,
    segment,
    age,
    name
from (
    select
        id,
        segment,
        age,
        name,
        row_number() over (partition by segment order by age, id) as rnk
    from customers
) as t
where rnk <= 3
order by name desc;
