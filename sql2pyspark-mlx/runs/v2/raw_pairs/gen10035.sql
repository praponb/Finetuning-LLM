select
    c.id,
    c.credit_limit,
    c.email,
    t.th_count
from customers c
inner join (select customer_id, count(case when country = 'TH' then 1 end) as th_count
    from sessions
    group by customer_id) t on c.id = t.customer_id
order by th_count;
