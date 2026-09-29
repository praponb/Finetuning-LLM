select id, status, monthly_fee
from (
    select *, row_number() over (partition by status order by monthly_fee, id) as pos
    from subscriptions
    where start_date between '2023-05-02' and '2025-09-01'
) as ranked
where pos <= 2
order by id desc;
