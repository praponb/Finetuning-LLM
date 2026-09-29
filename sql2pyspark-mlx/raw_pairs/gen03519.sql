with scored as (
    -- Running and moving aggregates
    select
        *,
        ntile(4) over (partition by carrier, order_id order by cost desc, id) as cost_bucket,
        first_value(carrier) ignore nulls over (order by delivered_date, id rows between unbounded preceding and current row) as first_known_carrier,
        rank() over (partition by status, order_id order by cost nulls first) as cost_rank
    from shipments
    where order_id = 26
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by carrier order by cost desc, id) = 1
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        status,
        cost_bucket,
        first_known_carrier,
        cost_rank,
        add_months(delivered_date, 6) as delivered_date_plus_months
    from ranked
    where order_id is not null
),
joined as (
    -- Attach orders attributes
    select
        src.*,
        d.channel,
        d.amount,
        d.status as order_status
    from valid src
    left join orders d
        on src.order_id = d.id
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from joined a
    inner join promotions pr
        on a.channel = pr.channel
       and a.delivered_date_plus_months between pr.starts_on and pr.ends_on
),
totals as (
    -- Multi-level aggregation
    select
        case when cost_rank >= 8 then 'PREMIUM' else 'STANDARD' end as cost_rank_tier,
        sort_array(collect_set(status)) as status_list,
        round(avg(cost_rank), 2) as avg_cost_rank
    from matched
    group by case when cost_rank >= 8 then 'PREMIUM' else 'STANDARD' end
    having avg_cost_rank >= 0
)
select cost_rank_tier, status_list
from totals
order by cost_rank_tier desc
