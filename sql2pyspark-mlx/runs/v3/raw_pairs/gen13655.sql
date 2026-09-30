with store_stats as (
    select store_id, sum(case when channel = 'partner' then 1 else 0 end) as partner_count
    from orders
    group by store_id
)
select stores.id, stores.opened_date, store_stats.partner_count
from stores
join store_stats on stores.id = store_stats.store_id
order by id asc;
