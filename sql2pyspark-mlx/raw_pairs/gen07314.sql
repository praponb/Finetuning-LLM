with valid as (
    select
        id,
        email,
        country,
        city,
        segment,
        credit_limit,
        concat(country, ', ', city) as country_city,
        round(credit_limit, 2) as credit_limit_rounded
    from customers
    where name like '%Dia%'
),
scored as (
    select
        *,
        sum(credit_limit_rounded) over w_grp as running_sum_credit_limit_rounded,
        min(credit_limit_rounded) over w_grp as running_min_credit_limit_rounded,
        max(credit_limit_rounded) over w_grp as running_max_credit_limit_rounded,
        row_number() over (order by credit_limit_rounded desc, id) as row_seq
    from valid
    where credit_limit_rounded between 24 and 52
    window w_grp as (partition by segment order by credit_limit, id rows between unbounded preceding and current row)
),
kept as (
    select s.*
    from scored s
    where exists (
            select 1
            from accounts c
            where c.customer_id = s.id
              and c.balance <= 52000
        )
),
enriched as (
    select
        id,
        email,
        country,
        city,
        credit_limit,
        running_max_credit_limit_rounded,
        row_seq,
        row_number() over (partition by city order by running_min_credit_limit_rounded, id) as event_seq,
        ntile(3) over (order by credit_limit_rounded desc, id) as credit_limit_rounded_bucket,
        lag(row_seq, 1) over (partition by segment order by running_sum_credit_limit_rounded, id) as prev_row_seq
    from kept
)
select
    id,
    email,
    city,
    credit_limit,
    row_seq,
    prev_row_seq
from enriched;
