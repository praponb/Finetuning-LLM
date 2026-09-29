with enriched as (
    select
        src.id,
        src.order_id,
        src.shipped_date,
        d.discount,
        d.channel
    from shipments src
    inner join orders d
        on src.order_id = d.id
)
select
    date_trunc('week', shipped_date) as week_start,
    percentile_approx(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
    round(sum(discount) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as discount_per_partner,
    count(*) as row_count
from enriched
group by date_trunc('week', shipped_date)
order by discount_per_partner desc
