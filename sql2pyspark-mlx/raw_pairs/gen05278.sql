with filtered as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        date_add(sale_date, 32) as sale_date_plus_32d,
        case when units >= 32 then 'High' when units >= 12 then 'Medium' else 'Low' end as units_tier
    from store_sales
    where sale_date > date '2023-06-20'
),
enriched as (
    select
        id,
        store_id,
        sale_date,
        units,
        sale_date_plus_32d,
        units_tier,
        sum(units) over (partition by product_id order by sale_date, id rows between unbounded preceding and current row) as cumulative_units,
        avg(units) over (partition by store_id order by sale_date, id rows between 6 preceding and 1 preceding) as units_avg_6_prior
    from filtered
    where units < 22
),
enriched_store_sales as (
    select
        e.id,
        e.store_id,
        e.sale_date,
        e.units,
        e.units_tier,
        e.units_avg_6_prior,
        d.region,
        d.sqft,
        d.opened_date
    from enriched e
    inner join stores d
        on e.store_id = d.id
),
deduped as (
    select
        id,
        store_id,
        units_avg_6_prior,
        sqft
    from (
        select *, row_number() over (partition by store_id order by units_avg_6_prior desc nulls first, id) as rn
        from enriched_store_sales
    ) t
    where rn = 1
),
enriched_store_sales_2 as (
    select
        s.id,
        s.store_id,
        p.sqft as store_sqft,
        p.store_name
    from deduped s
    left join stores p
        on s.store_id = p.id
),
scored as (
    select
        *,
        max(store_sqft) over (partition by store_id order by store_sqft, id rows between 2 preceding and current row) as last3_max_store_sqft,
        count(store_sqft) over (partition by store_id order by store_sqft, id rows between 2 preceding and current row) as last3_count_store_sqft,
        avg(store_sqft) over (partition by store_id order by store_sqft, id rows between 2 preceding and current row) as last3_avg_store_sqft,
        sum(store_sqft) over (partition by store_id order by store_sqft, id rows between unbounded preceding and current row) as running_store_sqft
    from enriched_store_sales_2
)
select
    dim.region,
    count(*) as num_rows,
    round(avg(a.running_store_sqft), 2) as avg_running_store_sqft
from scored a
left join stores dim on a.store_id = dim.id
group by dim.region;
