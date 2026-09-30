select
    s.id,
    s.device,
    s.customer_id,
    c.email,
    c.city,
    c.signup_date,
    initcap(trim(s.browser)) as browser_title
from sessions s
left outer join customers c on s.customer_id = c.id
where c.credit_limit >= 6700
order by s.device desc, s.id desc;
