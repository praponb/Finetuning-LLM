with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        amount,
        paid_at,
        initcap(trim(method)) as method_title,
        quarter(paid_at) as paid_at_quarter
    from payments
    where paid_at >= '2023-04-01'
      and paid_at > to_timestamp('2023-05-09 03:00:00')
),
joined as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.amount,
        e.method_title,
        b.channel,
        b.order_date,
        b.discount
    from valid e
    left join orders b
        on e.order_id = b.id
    where b.shipping_fee >= 18
),
scored as (
    -- Running and moving aggregates
    select
        id,
        amount,
        discount,
        count(discount) over w_part as last3_count_discount,
        avg(discount) over w_part as last3_avg_discount,
        min(discount) over w_part as last3_min_discount
    from joined
    window w_part as (partition by order_id order by discount, id rows between 2 preceding and current row)
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        least(last3_count_discount, discount) as least_last3_count_discount_discount,
        last3_avg_discount + last3_count_discount as last3_avg_discount_plus_last3_count_discount
    from scored
    where last3_avg_discount < 0.36
)
-- Multi-level aggregation
select
    case when least_last3_count_discount_discount >= 69 then 'HIGH' else 'LOW' end as least_last3_count_discount_discount_tier,
    percentile_approx(last3_count_discount, array(0.25, 0.5, 0.75)) as last3_count_discount_quartiles,
    approx_percentile(last3_avg_discount_plus_last3_count_discount, array(0.25, 0.5, 0.75)) as last3_avg_discount_plus_last3_count_discount_quartiles,
    percentile_approx(least_last3_count_discount_discount, array(0.25, 0.5, 0.75)) as least_last3_count_discount_discount_quartiles
from base
group by least_last3_count_discount_discount_tier
order by least_last3_count_discount_discount_tier desc nulls first
