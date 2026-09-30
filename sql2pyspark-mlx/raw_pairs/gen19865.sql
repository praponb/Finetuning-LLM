with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        datediff(current_date(), opened_date) days_since_opened_date
    from accounts
    where opened_date >= date '2023-04-12'
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        upper(account_type) account_type_upper,
        date_add(opened_date, 23) opened_date_plus_23d,
        coalesce(status, 'DIRECT') status_filled
    from cleaned
    where opened_date > to_date('2023-09-10')
),
valid_accounts as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        days_since_opened_date,
        account_type_upper,
        opened_date_plus_23d,
        status_filled,
        datediff(opened_date_plus_23d, opened_date) / 7 weeks_opened_date_to_opened_date_plus_23d,
        coalesce(status, 'UNKNOWN') status_clean
    from valid
    where balance is not null
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from valid_accounts e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.opened_date >= sub.start_date
       and e.opened_date < sub.end_date + interval 14 days
)
select
    id,
    account_type,
    balance,
    days_since_opened_date,
    opened_date_plus_23d,
    status_filled,
    subscription_id,
    monthly_fee
from with_window
order by opened_date_plus_23d desc nulls first;
