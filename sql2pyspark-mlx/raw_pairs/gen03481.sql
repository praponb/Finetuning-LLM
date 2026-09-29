with base as (
    -- Filter the raw rows and derive helper columns
    select *, weekofyear(end_date) end_date_weekofyear
    from subscriptions
    where start_date > date '2023-03-18'
      and status = 'CANCELLED'
),
scored as (
    -- Ranking and gap features
    select
        *,
        avg(monthly_fee) over (partition by customer_id order by end_date, id rows between 3 preceding and 1 preceding) monthly_fee_avg_3_prior,
        avg(monthly_fee) over (partition by plan order by end_date, id rows between 5 preceding and current row) monthly_fee_avg_5,
        round(monthly_fee / sum(monthly_fee) over (partition by month(end_date), plan), 4) monthly_fee_share,
        last_value(monthly_fee) ignore nulls over (order by monthly_fee, id rows between unbounded preceding and current row) last_known_monthly_fee,
        row_number() over (partition by customer_id order by start_date, id) row_seq
    from base
),
scored_subscriptions as (
    -- Ranking and gap features
    select
        *,
        min(monthly_fee_avg_3_prior) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) running_min_monthly_fee_avg_3_prior,
        avg(monthly_fee_avg_3_prior) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) running_avg_monthly_fee_avg_3_prior,
        sum(monthly_fee_avg_3_prior) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) running_sum_monthly_fee_avg_3_prior
    from scored
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        monthly_fee_avg_3_prior,
        monthly_fee_share,
        last_known_monthly_fee,
        row_seq,
        running_min_monthly_fee_avg_3_prior,
        running_sum_monthly_fee_avg_3_prior,
        cast(monthly_fee as int) monthly_fee_int
    from scored_subscriptions
    where start_date >= to_date('2023-08-15')
      and plan = 'basic'
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.plan,
        e.start_date,
        e.end_date,
        e.monthly_fee_avg_3_prior,
        e.monthly_fee_share,
        e.row_seq,
        e.monthly_fee_int,
        c.age,
        c.city
    from valid e
    join customers c
        on e.customer_id = c.id
    where c.country in ('SG', 'DE', 'TH')
),
kept_keys as (
    -- Set difference on keys
    select customer_id, plan
    from with_dims
    except
    select customer_id, plan
    from subscriptions
    where monthly_fee >= 200
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    join kept_keys k on s.customer_id = k.customer_id and s.plan = k.plan
)
select
    s.id,
    s.customer_id,
    s.plan,
    s.row_seq,
    s.monthly_fee_int,
    s.city,
    case when city rlike '^(Berlin|Tokyo)$' then 'GROUP_A' else 'CORE' end as city_zone,
    (select avg(t2.discount) from orders t2 where t2.customer_id = s.customer_id) as avg_order_discount
from verified s
