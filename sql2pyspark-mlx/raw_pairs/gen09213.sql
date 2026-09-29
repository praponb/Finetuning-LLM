with base as (
    select
        *,
        cast(rating as int) as rating_int,
        case when country = 'JP' then 1 else 0 end as is_jp
    from suppliers
    where city is distinct from 'Singapore'
      and supplier_name is not null
)
select *, row_number() over (partition by country order by rating desc nulls first, id) as rank_in_group
from base
qualify rank_in_group = 1
