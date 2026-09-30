with filtered as (
    select *, round(shipping_fee, 2) as shipping_fee_rounded
    from orders
    where order_date >= date '2023-09-24'
),
enriched as (
    select
        *,
        lag(discount) over (partition by status order by order_date, id) as prev_discount,
        lead(shipping_fee) over (partition by status order by order_date, id) as next_shipping_fee,
        lag(shipping_fee) over (partition by status order by order_date, id) as prev_shipping_fee,
        row_number() over (partition by status order by order_date, id) as seq_no,
        ntile(3) over (order by amount desc, id) as amount_bucket
    from filtered
),
eligible as (
    select s.*
    from enriched s
    where exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.unit_price <= 390
        )
),
remaining_keys as (
    select status
    from eligible
    except all
    select status
    from orders
    where amount between 720 and 800
),
kept_rows as (
    select s.*
    from eligible s
    inner join remaining_keys k on s.status = k.status
)
select
    dim.city,
    src.status,
    count(*) as rows_total,
    max(src.amount) as max_amount,
    grouping_id() as grouping_level
from kept_rows src
inner join customers dim on src.customer_id = dim.id
group by grouping sets ((dim.city, src.status), (dim.city))
order by max_amount desc nulls first;
