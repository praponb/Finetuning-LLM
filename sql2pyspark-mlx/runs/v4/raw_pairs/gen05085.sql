select *
from (
    select *, row_number() over (partition by merchant order by amount desc nulls last, id) as dedup_rank
    from transactions
) ranked
where dedup_rank = 1
