with deduped as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock
    from (
        select *, rank() over (partition by store_id order by quantity desc) as row_num
        from inventory
    ) x
    where row_num = 1
),
with_windows as (
    select
        *,
        lag(quantity) over w_running prev_quantity,
        lead(quantity) over w_running next_quantity,
        row_number() over w_running row_seq
    from deduped
    window w_running as (partition by store_id order by quantity, id)
),
with_windows_inventory as (
    select
        *,
        lead(next_quantity) over w_hist next_next_quantity,
        lag(quantity) over w_hist prev_quantity_2,
        lead(quantity) over w_hist next_quantity_2,
        row_number() over w_hist seq_no
    from with_windows
    where last_restock > '2025-01-07'
    window w_hist as (partition by store_id order by next_quantity, id)
),
filtered as (
    select
        id,
        store_id,
        quantity,
        last_restock,
        next_quantity,
        seq_no,
        floor(next_quantity) next_quantity_whole
    from with_windows_inventory
    where product_id = 5
),
enriched as (
    select
        s.*,
        p.opened_date,
        p.store_name,
        p.city
    from filtered s
    inner join stores p
        on s.store_id = p.id
),
enriched_inventory as (
    select
        s.id,
        s.store_id,
        s.quantity,
        s.last_restock,
        s.seq_no,
        s.opened_date,
        d.city as store_city,
        d.opened_date as store_opened_date,
        d.store_name as store_store_name
    from enriched s
    inner join stores d
        on s.store_id = d.id
)
select
    id,
    quantity,
    last_restock,
    seq_no,
    store_city,
    store_opened_date
from enriched_inventory;
