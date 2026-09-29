with latest as (
    select
        id,
        order_id,
        status
    from (
        select *, row_number() over (partition by status order by amount desc nulls first, id) as row_num
        from payments
    ) t
    where row_num = 1
),
enriched as (
    select
        e.id,
        e.order_id,
        e.status,
        d.discount,
        d.order_date,
        d.shipping_fee
    from latest e
    left join orders d
        on e.order_id = d.id
),
with_windows as (
    select
        id,
        order_id,
        status,
        discount,
        lead(discount, 2) over (partition by status order by order_date, id) as next_discount,
        row_number() over (partition by order_id, status order by order_date desc, id) as event_seq,
        sum(discount) over (order by shipping_fee, id rows between unbounded preceding and current row) as cumulative_discount,
        row_number() over (partition by status order by order_date desc, id) as row_seq,
        sum(discount) over (partition by status order by shipping_fee, id rows between unbounded preceding and current row) as cumulative_discount_2
    from enriched
),
scored as (
    select
        id,
        order_id,
        status,
        next_discount,
        event_seq,
        cumulative_discount_2,
        lead(event_seq) over (partition by status order by cumulative_discount_2, id) as next_event_seq,
        lag(event_seq) over (partition by status order by cumulative_discount_2, id) as prev_event_seq,
        lead(discount) over (partition by status order by cumulative_discount_2, id) as next_discount_2,
        row_number() over (partition by status order by cumulative_discount_2, id) as row_seq_2,
        percent_rank() over (order by event_seq desc) as event_seq_percent_rank
    from with_windows
),
with_dims as (
    select
        a.*,
        d.discount,
        d.status as order_status
    from scored a
    left join orders d
        on a.order_id <=> d.id
    where d.amount < 1400
)
select
    p.channel,
    s.status as payment_status,
    count(*) as rows_total,
    avg(s.event_seq) as avg_event_seq,
    max(s.event_seq_percent_rank) as max_event_seq_percent_rank
from with_dims s
inner join orders p on s.order_id = p.id
group by grouping sets ((p.channel, s.status), (p.channel), ())
