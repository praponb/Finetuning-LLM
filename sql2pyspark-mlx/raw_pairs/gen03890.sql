with enriched as (
    select
        src.*,
        p.cost,
        p.brand
    from inventory src
    left join products p
        on src.product_id = p.id
)
select
    case when quantity >= 218 then 'HIGH' else 'LOW' end as quantity_tier,
    store_id,
    max(cost) filter (where brand = 'Globex') as globex_max_cost,
    count(case when brand = 'Acme' then 1 end) as acme_count,
    sum(case when brand = 'Globex' then cost else 0 end) as globex_cost,
    count_if(brand = 'Globex') as globex_events,
    rank() over (partition by case when quantity >= 218 then 'HIGH' else 'LOW' end order by count(case when brand = 'Acme' then 1 end) desc) as acme_count_rank
from enriched
group by case when quantity >= 218 then 'HIGH' else 'LOW' end, store_id
order by globex_cost nulls first;
