with filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(quantity as string) as quantity_num,
        cast(quantity as double) as quantity_num_2
    from inventory
    where last_restock >= to_date('2023-09-17')
),
joined as (
    -- Attach products attributes
    select src.*, p.cost
    from filtered src
    left join products p
        on src.product_id = p.id
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(quantity_num_2) over w_hist as store_id_stddev_samp_quantity_num_2,
        cume_dist() over (partition by date_trunc('MONTH', last_restock) order by cost desc nulls last) as cost_cume_dist,
        row_number() over w_hist_2 as event_seq,
        avg(cost) over w_ordered as cost_avg_5_prior
    from joined
    where quantity < 124
    window w_hist as (partition by store_id), w_hist_2 as (partition by date_trunc('YEAR', last_restock), product_id order by cost, id), w_ordered as (partition by store_id order by quantity_num_2, id rows between 5 preceding and 1 preceding)
),
enriched as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.quantity,
        a.quantity_num,
        a.quantity_num_2,
        a.store_id_stddev_samp_quantity_num_2,
        d.city,
        d.sqft
    from with_windows a
    left join stores d
        on a.store_id = d.id
)
-- Join a dimension and aggregate
select
    d.city as store_city,
    d.region,
    src.city as inventory_city,
    count(*) as cnt,
    max(src.quantity_num_2) as max_quantity_num_2
from enriched src
join stores d on src.store_id = d.id
group by grouping sets ((d.city, d.region, src.city), (d.city, d.region), (d.city), ());
