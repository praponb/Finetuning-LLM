with valid as (
    -- Basic cleansing and defaults
    select *, substring(status, 1, 2) as status_prefix
    from payments
    where paid_at > date '2023-07-10'
),
enriched as (
    -- Attach orders attributes
    select a.*, o.discount
    from valid a
    inner join orders o
        on a.order_id <=> o.id
    where o.status = 'PENDING'
),
valid_payments as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        status_prefix,
        discount,
        to_date(paid_at) as paid_at_date,
        coalesce(method, 'N/A') as method_filled
    from enriched
    where paid_at >= date '2023-10-18'
),
remaining_keys as (
    -- Set difference on keys
    select method, order_id
    from valid_payments
    except
    select method, order_id
    from payments
    where amount is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid_payments s
    inner join remaining_keys k on s.method = k.method and s.order_id = k.order_id
)
select
    id,
    paid_at,
    status,
    status_prefix,
    discount,
    method_filled
from kept_rows
order by paid_at desc;
