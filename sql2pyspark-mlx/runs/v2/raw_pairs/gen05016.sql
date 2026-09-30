select *
from (
    select method, sum(case when method = 'cash' then 1 else 0 end) as cash_count, count(*) as row_count
    from payments
    group by method
) as sub
where row_count < 9;
