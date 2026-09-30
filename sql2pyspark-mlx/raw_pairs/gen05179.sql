with matched as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    left join promotions pr
        on coalesce(e.channel, 'store') = pr.channel
       and e.ends_on between pr.starts_on and pr.ends_on
)
select
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    promo_id,
    promotion_discount_pct,
    case when channel rlike '^(web|mobile)$' then 'GROUP_A' else 'PRIORITY' end as channel_zone
from matched
order by discount_pct desc
