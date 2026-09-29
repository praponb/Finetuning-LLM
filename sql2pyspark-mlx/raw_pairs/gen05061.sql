with scored as (
    select
        *,
        lead(monthly_fee) over (partition by plan order by end_date, id) as next_monthly_fee,
        row_number() over (partition by plan order by end_date, id) as event_seq
    from subscriptions
),
with_windows as (
    select
        id,
        customer_id,
        plan,
        monthly_fee,
        status,
        row_number() over (partition by plan, status order by monthly_fee, id) as seq_no,
        lead(next_monthly_fee, 2) over (partition by plan order by end_date, id) as next_next_monthly_fee,
        sum(monthly_fee) over w_part as cumulative_monthly_fee
    from scored
    window w_part as (partition by customer_id order by end_date, id rows between unbounded preceding and current row)
),
enriched as (
    select
        *,
        avg(seq_no) over (partition by plan order by monthly_fee, id rows between 6 preceding and current row) as last7_avg_seq_no,
        min(seq_no) over (partition by plan order by monthly_fee, id rows between 6 preceding and current row) as last7_min_seq_no,
        max(seq_no) over (partition by plan order by monthly_fee, id rows between 6 preceding and current row) as last7_max_seq_no,
        count(seq_no) over (partition by plan order by monthly_fee, id rows between 6 preceding and current row) as last7_count_seq_no
    from with_windows
),
remaining_keys as (
    select id
    from enriched
    intersect all
    select id
    from subscriptions
    where status = 'EXPIRED'
),
reconciled as (
    select s.*
    from enriched s
    inner join remaining_keys k on s.id = k.id
),
enriched_subscriptions as (
    select
        s.customer_id,
        s.plan,
        s.next_next_monthly_fee,
        s.cumulative_monthly_fee,
        s.last7_avg_seq_no,
        s.last7_max_seq_no,
        s.last7_count_seq_no,
        d.name,
        d.signup_date
    from reconciled s
    left join customers d
        on s.customer_id = d.id
),
with_dims as (
    select a.*, c.segment
    from enriched_subscriptions a
    left join customers c
        on a.customer_id = c.id
)
select
    s.customer_id,
    s.plan,
    s.last7_avg_seq_no,
    s.last7_max_seq_no,
    s.last7_count_seq_no,
    s.segment,
    case when plan rlike '^(enterprise|basic)$' then 'OTHER_ZONE' when plan rlike '^(standard|premium)$' then 'GROUP_A' else 'CORE' end as plan_zone,
    (select max(x.amount) from orders x where x.customer_id = s.customer_id) as max_order_amount
from with_dims s;
