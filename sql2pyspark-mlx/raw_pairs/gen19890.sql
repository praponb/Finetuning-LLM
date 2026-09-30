with valid as (
    -- Keep recent, valid rows
    select
        *,
        case when carrier in ('Kerry', 'Flash') then 'Tier 1' else 'Other' end as carrier_group,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from shipments
    where carrier is not null
      and carrier is distinct from 'UPS'
)
-- Keep one row per key (latest first)
select *, rank() over (partition by carrier order by cost desc) as rank_in_group
from valid
qualify rank_in_group = 1;
