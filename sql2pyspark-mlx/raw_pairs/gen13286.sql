select *
from (
    select is_flagged, count(case when txn_type = 'CREDIT' then 1 end) as credit_count
    from transactions
    where is_flagged
    group by is_flagged
) as agg
where credit_count >= 9
order by credit_count asc, is_flagged asc
limit 20;
