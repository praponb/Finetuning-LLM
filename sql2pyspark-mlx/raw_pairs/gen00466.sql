with valid as (
    select
        id,
        store_id,
        last_day(last_restock) as last_restock_month_end
    from inventory
    where quantity between 101 and 108
),
scored as (
    select
        id,
        store_id,
        row_number() over w_ordered as seq_no,
        row_number() over (partition by date_trunc('WEEK', last_restock_month_end), store_id order by last_restock_month_end desc, id) as event_seq,
        row_number() over w_hist as event_seq_2,
        row_number() over w_ordered as event_seq_3,
        row_number() over w_ordered as event_seq_4
    from valid
    window w_ordered as (partition by store_id order by last_restock_month_end desc, id), w_hist as (partition by store_id order by last_restock_month_end, id)
),
enriched as (
    select
        e.*,
        d.store_name,
        d.city
    from scored e
    left join stores d
        on e.store_id = d.id
),
joined as (
    select s.*, p.opened_date
    from enriched s
    left join stores p
        on s.store_id = p.id
),
remaining_keys as (
    select id
    from joined
    except all
    select id
    from inventory
    where month(last_restock) in (3, 7, 10)
),
reconciled as (
    select s.*
    from joined s
    inner join remaining_keys k on s.id = k.id
)
select
    d.region,
    s.city as inventory_city,
    count(*) as rows_total,
    max(s.event_seq) as max_event_seq
from reconciled s
left join stores d on s.store_id = d.id
group by grouping sets ((d.region, s.city), (d.region), ())
