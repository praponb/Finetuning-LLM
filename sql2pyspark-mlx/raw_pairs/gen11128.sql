with cleaned as (
    select
        *,
        lower(segment) segment_lower,
        coalesce(segment, 'N/A') segment_clean
    from customers
    where age is not null
),
with_windows as (
    select
        *,
        lead(credit_limit) over w_recent next_credit_limit,
        lag(age) over w_recent prev_age,
        row_number() over w_recent row_seq,
        first_value(credit_limit, true) over (order by signup_date, id rows between unbounded preceding and current row) first_known_credit_limit
    from cleaned
    window w_recent as (partition by segment, country order by age, id)
),
with_last as (
    select
        s.id,
        s.name,
        s.country,
        s.city,
        s.segment,
        s.is_active,
        s.age,
        s.segment_lower,
        s.segment_clean,
        s.next_credit_limit,
        s.row_seq,
        x.amount
    from with_windows s
    left join lateral (
            select c.amount
            from orders c
            where c.customer_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) x on true
),
with_last_customers as (
    select
        s.*,
        x.session_browser,
        x.session_country
    from with_last s
    left join lateral (
            select c.browser as session_browser, c.country as session_country
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    select
        id,
        name,
        country,
        city,
        is_active,
        age,
        segment_lower,
        segment_clean,
        amount,
        session_country
    from (
        select *, rank() over (partition by city order by age desc nulls first) as latest_rank
        from with_last_customers
    ) ranked
    where latest_rank = 1
)
select
    case when age >= 61 then 'PREMIUM' else 'STANDARD' end as age_tier,
    sum(age) filter (where country = 'US') as us_total_age,
    size(collect_set(city)) as num_cities,
    round(sum(age) / nullif(count(case when country = 'TH' then 1 end), 0), 2) as age_per_th,
    count(*) filter (where session_country = 'DE') as de_rows,
    avg(amount) as avg_amount
from ranked
group by rollup (case when age >= 61 then 'PREMIUM' else 'STANDARD' end)
order by avg_amount nulls first;
