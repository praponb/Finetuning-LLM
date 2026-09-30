select id, account_type, opened_date
from (
    select *, row_number() over (partition by account_type order by opened_date, id) as row_num
    from accounts
    where opened_date <= '2023-09-18'
) as ranked
where row_num <= 3
order by account_type desc;
