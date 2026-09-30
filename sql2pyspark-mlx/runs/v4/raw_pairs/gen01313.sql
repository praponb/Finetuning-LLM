with ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id order by quantity desc, id) as row_num
        from inventory
    ) t
    where row_num = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        avg(quantity) over (partition by product_id, store_id order by quantity, id rows between 6 preceding and 1 preceding) as quantity_avg_6_prior,
        lead(quantity, 1) over w_hist as next_quantity,
        row_number() over (order by last_restock, id) as row_seq,
        avg(quantity) over (partition by store_id order by row_num, id rows between 6 preceding and current row) as quantity_avg_6,
        count(quantity) over w_running as store_id_count_quantity
    from ranked
    window w_hist as (partition by store_id order by row_num, id), w_running as (partition by store_id)
),
with_dims as (
    -- Attach products attributes
    select
        e.*,
        p.launch_date,
        p.brand
    from enriched e
    join products p
        on e.product_id = p.id
)
select
    s.id,
    s.store_id,
    s.row_num,
    s.quantity_avg_6_prior,
    s.next_quantity,
    s.row_seq,
    s.quantity_avg_6,
    s.store_id_count_quantity,
    s.launch_date,
    (select sum(x.revenue) from store_sales x where x.store_id = s.store_id) as sum_store_sale_revenue
from with_dims s
order by quantity_avg_6 desc nulls last;
