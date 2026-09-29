with with_dims as (
    -- Attach orders attributes
    select
        s.*,
        b.amount as order_amount,
        b.shipping_fee
    from payments s
    left join orders b
        on s.order_id = b.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        method,
        amount,
        shipping_fee,
        initcap(trim(status)) as status_title
    from with_dims
    where paid_at >= timestamp '2023-08-19 09:00:00'
      and order_id is not null
),
kept_keys as (
    -- Set difference on keys
    select method, order_id
    from valid
    except all
    select method, order_id
    from payments
    where amount <= 1560
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join kept_keys k on s.method = k.method and s.order_id = k.order_id
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        amount - shipping_fee as amount_with_shipping,
        case when method = 'cash' then 1 else 0 end as is_cash,
        coalesce(method, 'OTHER') as resolved_method
    from kept_rows
    where shipping_fee < 33
),
base_payments as (
    -- Basic cleansing and defaults
    select
        *,
        case when method in ('cash', 'promptpay') then 'Tier 1' else 'Other' end as method_group,
        round(amount_with_shipping * 1.1, 2) as amount_with_shipping_scaled,
        coalesce(method, 'UNKNOWN') as method_clean
    from base
    where method <> 'bank_transfer'
)
select
    id,
    method,
    amount,
    shipping_fee,
    amount_with_shipping,
    is_cash,
    resolved_method,
    method_group,
    method_clean,
    case when method rlike '^(promptpay|paypal)$' then 'EXTENDED' when method rlike '^(card|bank_transfer)$' then 'CORE' else 'PRIORITY' end as method_zone
from base_payments;
