with base as (
    select
        id,
        store_id,
        product_id,
        last_restock,
        cast(quantity as string) as quantity_num
    from inventory
    where last_restock > to_date('2023-02-12')
),
with_windows as (
    select
        id,
        store_id,
        product_id,
        last_restock,
        quantity_num,
        row_number() over (partition by product_id order by last_restock asc, id asc) as event_seq,
        row_number() over (partition by store_id, product_id order by last_restock desc, id asc) as seq_no,
        row_number() over (partition by product_id order by last_restock asc, id asc) as row_seq,
        row_number() over (partition by product_id order by last_restock asc, id asc) as row_seq_2,
        row_number() over (order by last_restock desc, id asc) as seq_no_2
    from base
),
matched_keys as (
    select id, store_id
    from with_windows
    except
    select id, store_id
    from inventory
    where year(last_restock) >= 2024
),
reconciled as (
    select s.*
    from with_windows s
    join matched_keys k on s.id = k.id and s.store_id = k.store_id
),
kept_keys as (
    select product_id
    from reconciled
    except distinct
    select product_id
    from inventory
    where year(last_restock) >= 2024
),
kept_rows as (
    select s.*
    from reconciled s
    join kept_keys k on s.product_id = k.product_id
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.quantity_num,
    s.seq_no,
    s.row_seq,
    s.seq_no_2,
    (select max(t2.quantity) from inventory t2 where t2.product_id = s.product_id) as max_inventory_quantity
from kept_rows s
