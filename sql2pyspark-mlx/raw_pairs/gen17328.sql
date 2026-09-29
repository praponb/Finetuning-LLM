with numbered as (
    select *, row_number() over (partition by account_id order by amount desc, id) as row_num
    from transactions
)
select id, account_id, amount
from numbered
where row_num <= 2
order by amount desc
