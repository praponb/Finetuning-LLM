select id, country, credit_limit
from (
    select
        id,
        country,
        credit_limit,
        row_number() over (partition by country order by credit_limit desc, id) as rnk
    from customers
) as ranked
where rnk <= 3
order by 3
