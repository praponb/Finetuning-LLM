with enriched as (
    -- Attach orders attributes
    select
        e.*,
        b.discount,
        b.shipping_fee
    from shipments e
    left join orders b
        on e.order_id = b.id
    where b.discount >= 0.3
),
with_dims as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.shipped_date,
        src.delivered_date,
        src.cost,
        src.status,
        src.discount,
        p.discount as order_discount
    from enriched src
    inner join orders p
        on src.order_id = p.id
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by order_discount desc, id) as latest_rank
    from with_dims
    qualify latest_rank = 1
),
enriched_shipments as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over w_part as row_seq,
        lead(order_discount, 1, 0) over (partition by month(shipped_date), order_id order by cost, id) as next_order_discount,
        first_value(order_discount) ignore nulls over w_running as first_known_order_discount
    from ranked
    window w_part as (partition by status order by cost desc, id), w_running as (partition by status, order_id order by cost, id rows between unbounded preceding and current row)
),
kept_keys as (
    -- Set difference on keys
    select order_id, status
    from enriched_shipments
    except distinct
    select order_id, status
    from shipments
    where order_id in (5, 6, 12, 24)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_shipments s
    inner join kept_keys k on s.order_id = k.order_id and s.status = k.status
),
base as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        delivered_date,
        status,
        discount,
        order_discount,
        next_order_discount,
        first_known_order_discount,
        case when status in ('IN_TRANSIT', 'RETURNED') then 'Core' else 'Other' end as status_group
    from verified
    where delivered_date >= date '2023-03-31'
      and order_id is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        discount,
        next_order_discount,
        first_known_order_discount,
        rank() over (partition by status order by first_known_order_discount desc nulls last) as first_known_order_discount_rank
    from base
)
-- Grouped metrics
select
    case when discount >= 0.33 then 'PREMIUM' else 'STANDARD' end as discount_tier,
    case when first_known_order_discount_rank >= 7 then 'HIGH' else 'LOW' end as first_known_order_discount_rank_tier,
    avg(discount) as avg_discount,
    max(first_known_order_discount_rank) as max_first_known_order_discount_rank,
    count(*) as cnt,
    round(avg(discount), 2) as avg_discount_2,
    percentile_approx(first_known_order_discount, array(0.25, 0.5, 0.75)) as first_known_order_discount_quartiles,
    count(*) as event_count
from with_windows
group by case when discount >= 0.33 then 'PREMIUM' else 'STANDARD' end, case when first_known_order_discount_rank >= 7 then 'HIGH' else 'LOW' end
having avg_discount_2 >= 0
order by avg_discount
