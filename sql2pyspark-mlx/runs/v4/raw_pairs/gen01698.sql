with valid as (
    -- Keep recent, valid rows
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        lower(name) as name_lower
    from customers
    where signup_date > date '2023-10-31'
      and segment is not null
      and is_active
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by country order by credit_limit nulls last, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        name_lower,
        latest_rank,
        count(credit_limit) over (partition by country) as country_count_credit_limit,
        lag(credit_limit, 2) over (partition by city order by credit_limit, id) as prev_credit_limit,
        sum(credit_limit) over (order by signup_date, id rows between unbounded preceding and current row) as running_credit_limit,
        avg(credit_limit) over w_grp as credit_limit_avg_6,
        row_number() over w_recent as row_seq
    from deduped
    window w_grp as (partition by country, city order by latest_rank, id rows between 6 preceding and current row), w_recent as (partition by segment, country order by latest_rank, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.opened_date >= s.signup_date
        )
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(country_count_credit_limit) over (partition by city, segment) as city_avg_country_count_credit_limit,
        row_number() over (order by country_count_credit_limit desc, id) as row_seq_2,
        ntile(5) over (partition by segment order by row_seq desc, id) as row_seq_bucket,
        ntile(10) over (partition by country order by row_seq, id) as row_seq_bucket_2
    from kept
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        substring(city, 1, 3) as city_prefix,
        ceil(latest_rank) as latest_rank_whole,
        coalesce(segment, 'DIRECT') as resolved_segment
    from scored
    where signup_date > date '2023-12-24'
      and segment is not null
),
scored_customers as (
    -- Window metrics over several different partitions
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        name_lower,
        latest_rank,
        country_count_credit_limit,
        prev_credit_limit,
        running_credit_limit,
        credit_limit_avg_6,
        row_seq,
        city_avg_country_count_credit_limit,
        row_seq_2,
        row_seq_bucket,
        row_seq_bucket_2,
        city_prefix,
        latest_rank_whole,
        resolved_segment,
        round(country_count_credit_limit / sum(country_count_credit_limit) over (partition by country, city), 4) as country_count_credit_limit_share,
        first_value(row_seq_2) ignore nulls over (order by prev_credit_limit, id rows between unbounded preceding and current row) as first_known_row_seq_2
    from filtered
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        segment,
        country_count_credit_limit_share,
        credit_limit
    from scored_customers
) src
unpivot include nulls (
    val for measure in (country_count_credit_limit_share as COUNTRY_COUNT_CREDIT_LIMIT_SHARE, credit_limit as CREDIT_LIMIT)
);
