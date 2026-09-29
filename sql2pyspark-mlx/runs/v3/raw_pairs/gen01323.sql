select
    c.id,
    c.age,
    c.credit_limit,
    t.browser_values
from customers c
inner join (select customer_id, concat_ws(', ', sort_array(collect_set(browser))) as browser_values
    from sessions
    group by customer_id) t on c.id = t.customer_id
order by 2 desc;
