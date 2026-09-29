select *
from (
    select account_id, sum(case when merchant = 'Grab' then amount else 0 end) as grab_amount
    from transactions
    group by account_id
) as t
where grab_amount > 32
order by account_id desc;
