with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        length(name) as name_length
    from customers
    where signup_date >= date '2023-04-06'
      and city is not null
),
valid_customers as (
    -- Basic cleansing and defaults
    select
        id,
        email,
        country,
        segment,
        signup_date,
        credit_limit,
        name_length,
        credit_limit + name_length as credit_limit_plus_name_length,
        coalesce(segment, 'N/A') as segment_filled
    from valid
    where signup_date >= to_date('2023-09-07')
      and segment is not distinct from 'Consumer'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        country,
        segment,
        name_length
    from (
        select *, row_number() over (partition by country order by name_length nulls first, id) as rank_in_group
        from valid_customers
    ) t
    where rank_in_group = 1
),
latest_match as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.status,
        top1.balance
    from ranked s
    join lateral (
            select c.status, c.balance
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) top1 on true
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        country,
        segment,
        status,
        balance,
        coalesce(status, 'unassigned') as status_filled
    from latest_match
    where segment = 'Consumer'
),
summary as (
    -- Grouped metrics
    select
        country,
        segment,
        case when balance >= 82000 then 'LARGE' else 'SMALL' end as balance_tier,
        count(*) as num_rows,
        count(*) filter (where country = 'UK') as uk_rows,
        round(sum(balance) / nullif(count(case when country = 'SG' then 1 end), 0), 2) as balance_per_sg,
        count(*) filter (where segment = 'SMB') as smb_rows,
        dense_rank() over (partition by country order by count(*) filter (where country = 'UK') desc) as uk_rows_dense_rank
    from base
    group by country, segment, case when balance >= 82000 then 'LARGE' else 'SMALL' end
)
select
    country,
    segment,
    balance_tier,
    num_rows,
    balance_per_sg,
    smb_rows,
    uk_rows_dense_rank,
    case when segment rlike '^(Corporate|Consumer)$' then 'EXTENDED' when segment rlike '^(SMB)$' then 'GROUP_A' else 'PRIORITY' end as segment_zone
from summary
