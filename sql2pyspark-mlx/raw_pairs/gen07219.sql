with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        cast(is_active as int) as is_active_flag,
        coalesce(city, 'NONE') as city_filled
    from customers
    where credit_limit between 8900 and 12700
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify row_number() over (partition by country order by signup_date desc nulls first, id) = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        first_value(city, true) over (order by credit_limit, id rows between unbounded preceding and current row) as first_known_city,
        last_value(age) ignore nulls over (partition by city order by credit_limit, id rows between unbounded preceding and current row) as last_known_age,
        corr(age, credit_limit) over (partition by city, country) as corr_age_credit_limit,
        avg(age) over (partition by segment order by is_active_flag, id rows between 6 preceding and 1 preceding) as age_avg_6_prior
    from latest
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.city,
        s.segment,
        s.is_active,
        s.credit_limit,
        s.age,
        s.last_known_age,
        s.corr_age_credit_limit,
        s.age_avg_6_prior,
        x.amount,
        x.order_order_date
    from with_windows s
    left join lateral (
            select c.amount, c.order_date as order_order_date
            from orders c
            where c.customer_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Ranking and gap features
    select
        *,
        last_value(city) ignore nulls over (partition by city order by order_order_date, id rows between unbounded preceding and current row) as last_known_city,
        sum(corr_age_credit_limit) over (order by credit_limit, id rows between unbounded preceding and current row) as running_corr_age_credit_limit
    from latest_match
)
-- Native SQL PIVOT
select *
from (
    select
        segment,
        city,
        running_corr_age_credit_limit
    from enriched
) src
pivot (
    sum(running_corr_age_credit_limit)
    for city in ('Berlin', 'Singapore', 'Tokyo')
);
