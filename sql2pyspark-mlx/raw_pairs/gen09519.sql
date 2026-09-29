with scored as (
    select
        id,
        product_name,
        launch_date,
        min(price) over w_running as last7_min_price,
        sum(price) over w_running as last7_sum_price,
        count(price) over w_running as last7_count_price
    from products
    window w_running as (partition by category, brand order by launch_date, id rows between 6 preceding and current row)
),
not_flagged as (
    select s.*
    from scored s
    where not exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.unit_price > 230
        )
),
scored_products as (
    select
        id,
        product_name,
        launch_date,
        last7_sum_price,
        sum(last7_sum_price) over w_hist as running_last7_sum_price,
        sum(last7_count_price) over w_hist as running_last7_count_price
    from not_flagged
    window w_hist as (order by launch_date, id rows between unbounded preceding and current row)
),
latest_match as (
    select
        s.*,
        latest.quantity,
        latest.order_item_unit_price
    from scored_products s
    left join lateral (
            select c.quantity, c.unit_price as order_item_unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    product_name,
    launch_date,
    running_last7_sum_price,
    running_last7_count_price,
    quantity,
    order_item_unit_price,
    case when product_name rlike '^(Backpack XL|Yoga Mat)$' then 'CORE' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'OTHER_ZONE' else 'PRIORITY' end as product_name_zone
from latest_match;
