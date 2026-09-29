with deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by country, city order by credit_limit desc, id asc) as dedup_rank
        from customers
    ) ranked
    where dedup_rank = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(age, true) over (partition by segment order by dedup_rank asc, id asc rows between unbounded preceding and current row) as last_known_age,
        avg(age) over (order by dedup_rank asc, id asc rows between 2 preceding and 1 preceding) as age_avg_2_prior
    from deduped
),
matched_keys as (
    -- Set intersection on keys
    select id, country
    from scored
    intersect
    select id, country
    from customers
    where segment in ('Consumer', 'SMB')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join matched_keys k on s.id = k.id and s.country = k.country
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        floor(credit_limit) as credit_limit_whole,
        credit_limit + age_avg_2_prior as credit_limit_plus_age_avg_2_prior
    from reconciled
    where signup_date >= date '2023-10-13'
      and city is not null
      and last_known_age > 51
)
select
    id,
    name,
    country,
    segment,
    signup_date,
    is_active,
    age,
    dedup_rank,
    last_known_age
from filtered
order by country asc, signup_date asc
