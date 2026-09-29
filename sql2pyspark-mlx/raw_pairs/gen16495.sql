with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by country order by signup_date nulls first, id) as dedup_rank
        from customers
    ) ranked
    where dedup_rank = 1
)
select
    id,
    email,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age
from deduped
order by id nulls last;
