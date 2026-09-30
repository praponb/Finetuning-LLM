with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        coalesce(browser, 'Unknown') as browser_filled
    from sessions
    where started_at >= timestamp '2023-02-20 05:00:00'
      and browser is not null
),
with_dims as (
    -- Attach customers attributes
    select s.*, c.name
    from cleaned s
    left join customers c
        on s.customer_id = c.id
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        p.signup_date,
        p.name as customer_name
    from with_dims s
    left join customers p
        on s.customer_id = p.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        browser_filled,
        row_number() over w_hist as seq_no,
        row_number() over w_hist as seq_no_2,
        row_number() over (partition by customer_id order by signup_date desc, id) as seq_no_3
    from enriched
    where upper(name) like '%ANY%'
    window w_hist as (partition by customer_id order by signup_date, id)
),
with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.browser_filled,
        s.seq_no_2,
        s.seq_no_3,
        top1.web_event_event_type
    from with_windows s
    left join lateral (
            select c.event_type as web_event_event_type
            from web_events c
            where c.session_id = s.id
            order by c.event_time desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    browser_filled,
    seq_no_2,
    seq_no_3
from with_latest
order by id desc nulls last;
