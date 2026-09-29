with valid as (
    -- Keep recent, valid rows
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        datediff(delivered_date, shipped_date) as days_shipped_date_to_delivered_date
    from shipments
    where shipped_date <= '2024-02-24'
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by carrier, status order by cost nulls last, id) as row_num
        from valid
    ) t
    where row_num = 1
),
matched_keys as (
    -- Set difference on keys
    select status, id
    from latest
    except
    select status, id
    from shipments
    where shipped_date <= '2025-10-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join matched_keys k on s.status = k.status and s.id = k.id
),
remaining_keys as (
    -- Set intersection on keys
    select status
    from kept_rows
    intersect all
    select status
    from shipments
    where cost <= 32
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    inner join remaining_keys k on s.status = k.status
)
-- Multi-level aggregation
select
    date_trunc('year', delivered_date) as year_start,
    size(collect_set(carrier)) as num_carriers,
    count_if(carrier = 'Kerry') as kerry_events,
    size(collect_set(carrier)) as num_carriers_2,
    min(cost) as min_cost
from reconciled
group by year_start
having num_carriers >= 1
order by year_start desc
