with numbered as (
    select *, row_number() over (partition by account_type order by opened_date, id) as row_num
    from accounts
    where opened_date between '2024-04-01' and '2025-07-29'
)
select
    id,
    account_type,
    opened_date,
    customer_id
from numbered
where row_num <= 3;
