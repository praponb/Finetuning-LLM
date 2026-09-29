with deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        order_id,
        method
    from payments
    qualify row_number() over (partition by order_id order by paid_at nulls last, id) = 1
)
-- Aggregate with conditional metrics
select
    order_id,
    count(*) filter (where method = 'card') as card_rows,
    sort_array(collect_set(method)) as method_list,
    count(*) filter (where method = 'paypal') as paypal_rows
from deduped
group by order_id
having count(*) filter (where method = 'card') > 1
