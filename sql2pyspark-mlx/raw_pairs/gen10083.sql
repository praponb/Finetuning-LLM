with enriched as (
    select
        *,
        lead(rating) over (partition by city order by rating, id) as next_rating,
        row_number() over (partition by city order by rating, id) as event_seq
    from suppliers
)
select
    id,
    supplier_name,
    country,
    city,
    rating,
    event_seq,
    case when country rlike '^(TH|DE)$' then 'PRIORITY' when country rlike '^(US|SG)$' then 'OTHER_ZONE' else 'GROUP_A' end as country_zone
from enriched
order by rating desc nulls first
