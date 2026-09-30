with with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.quantity,
        latest.inventory_last_restock
    from stores s
    join lateral (
            select c.quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        quantity,
        inventory_last_restock,
        month(opened_date) as opened_date_month
    from with_latest
    where region is not null
      and opened_date <= '2025-08-01'
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from cleaned s
    where exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date > '2024-03-15'
        )
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        region,
        inventory_last_restock,
        max(opened_date_month) over w_part as last4_max_opened_date_month,
        sum(opened_date_month) over w_part as last4_sum_opened_date_month,
        avg(opened_date_month) over w_part as last4_avg_opened_date_month
    from eligible
    window w_part as (partition by region order by quantity asc, id asc rows between 3 preceding and current row)
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from metrics s
    where exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date >= s.inventory_last_restock
        )
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from kept
    qualify row_number() over (partition by region order by last4_avg_opened_date_month desc nulls first, id asc) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        lead(last4_sum_opened_date_month, 2) over (partition by region order by last4_avg_opened_date_month asc, id asc) as next_last4_sum_opened_date_month,
        sum(last4_avg_opened_date_month) over (partition by region order by last4_avg_opened_date_month asc, id asc rows between unbounded preceding and current row) as running_last4_avg_opened_date_month
    from ranked
)
select
    id,
    store_name,
    region,
    last4_max_opened_date_month,
    last4_sum_opened_date_month,
    last4_avg_opened_date_month,
    next_last4_sum_opened_date_month,
    running_last4_avg_opened_date_month
from enriched
order by last4_avg_opened_date_month desc, running_last4_avg_opened_date_month asc
