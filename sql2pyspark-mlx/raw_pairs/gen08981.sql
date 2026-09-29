with filtered as (
    select
        *,
        case when account_type in ('CHECKING', 'CREDIT') then 'Tier 1' else 'Other' end as account_type_group,
        initcap(trim(account_type)) as account_type_title,
        coalesce(status, 'UNKNOWN') as status_clean
    from accounts
    where opened_date > to_date('2023-07-21')
      and customer_id is not null
),
remaining_keys as (
    select status
    from filtered
    except
    select status
    from accounts
    where customer_id = 32
),
kept_rows as (
    select s.*
    from filtered s
    inner join remaining_keys k on s.status = k.status
),
scored as (
    select
        customer_id,
        status,
        status_clean,
        rank() over (order by balance desc) as balance_rank,
        percent_rank() over (order by balance desc nulls last) as balance_percent_rank,
        min(balance) over (partition by status, customer_id) as status_min_balance
    from kept_rows
    where status = 'FROZEN'
),
joined as (
    select
        src.*,
        c.signup_date,
        c.email,
        c.age
    from scored src
    left join customers c
        on src.customer_id = c.id
)
select
    dim.country,
    dim.city,
    count(*) as cnt,
    avg(src.balance_rank) as avg_balance_rank
from joined src
inner join customers dim on src.customer_id = dim.id
group by dim.country, dim.city
order by cnt nulls last;
