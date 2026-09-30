with with_windows as (
    -- Running and moving aggregates
    select
        *,
        lead(revenue) over w_hist as next_revenue,
        lead(units) over w_hist as next_units,
        lag(units) over w_hist as prev_units,
        row_number() over w_hist as row_seq,
        sum(units) over (partition by store_id order by units, id rows between unbounded preceding and current row) as running_units
    from store_sales
    window w_hist as (partition by product_id order by units, id)
),
matched_keys as (
    -- Set intersection on keys
    select store_id
    from with_windows
    intersect
    select store_id
    from store_sales
    where product_id = 6
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join matched_keys k on s.store_id = k.store_id
),
enriched as (
    -- Attach stores attributes
    select
        src.*,
        s.opened_date,
        s.store_name,
        s.region
    from verified src
    inner join stores s
        on src.store_id = s.id
    where s.region not in ('North', 'East', 'West')
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        sale_date,
        units,
        next_revenue,
        next_units,
        prev_units,
        row_seq,
        opened_date,
        store_name,
        region,
        units + next_units as units_plus_next_units
    from enriched
    where sale_date > to_date('2023-08-02')
      and region is not null
),
scored as (
    -- Window metrics over several different partitions
    select *, rank() over (order by units) as units_rank
    from base
)
select
    s.product_id,
    s.sale_date,
    s.units,
    s.next_revenue,
    s.next_units,
    s.opened_date,
    s.store_name,
    s.region,
    s.units_plus_next_units,
    case when store_name rlike '^(Airport Kiosk|Downtown Express)$' then 'GROUP_B' when store_name rlike '^(Siam Flagship|Mall Central)$' then 'OTHER_ZONE' else 'EXTENDED' end as store_name_zone,
    (select max(t2.quantity) from inventory t2 where t2.product_id = s.product_id) as max_inventory_quantity
from scored s
order by next_revenue nulls first
