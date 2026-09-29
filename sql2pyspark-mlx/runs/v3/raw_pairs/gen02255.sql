select segment, city, jp_credit_limit
from (
    select segment, city, sum(case when country = 'JP' then credit_limit else 0 end) as jp_credit_limit
    from customers
    group by segment, city
) as s
where jp_credit_limit < 74
order by city, segment
limit 5;
