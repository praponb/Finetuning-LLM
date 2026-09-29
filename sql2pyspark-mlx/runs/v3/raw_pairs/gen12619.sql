select id, status, opened_date
from (
    select
        id,
        status,
        opened_date,
        row_number() over (partition by status order by opened_date desc, id) as rn
    from accounts
    where balance is null
) as ranked
where rn <= 3
order by status, opened_date;
