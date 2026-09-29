with filtered as (
    select
        *,
        substring(browser, 1, 4) as browser_prefix,
        coalesce(country, 'DIRECT') as country_filled
    from sessions
    where country is distinct from 'SG'
      and customer_id in (22, 32, 39)
),
filtered_sessions as (
    select
        *,
        date_trunc('year', started_at) as started_at_period,
        initcap(trim(browser)) as browser_title,
        coalesce(browser, 'NONE') as browser_clean
    from filtered
    where browser is not distinct from 'Safari'
      and browser is not null
),
with_dims as (
    select
        e.*,
        p.credit_limit,
        p.country as customer_country,
        p.age
    from filtered_sessions e
    left join customers p
        on e.customer_id = p.id
),
ranked as (
    select *, row_number() over (partition by browser, device order by age, id) as rank_in_group
    from with_dims
    qualify rank_in_group = 1
),
remaining_keys as (
    select customer_id
    from ranked
    except
    select customer_id
    from sessions
    where browser not in ('Safari', 'Chrome')
),
verified as (
    select s.*
    from ranked s
    join remaining_keys k on s.customer_id = k.customer_id
),
enriched as (
    select
        *,
        dense_rank() over (order by credit_limit nulls last) as credit_limit_dense_rank,
        rank() over (partition by country, browser order by credit_limit desc nulls first) as credit_limit_rank,
        percent_rank() over w_ordered as credit_limit_percent_rank
    from verified
    window w_ordered as (partition by to_date(started_at), device order by credit_limit nulls last)
)
select
    browser,
    count(case when browser = 'Edge' then 1 end) as edge_count,
    round(avg(age), 2) as avg_age,
    max(started_at_period) as last_seen,
    round(sum(credit_limit) / nullif(count(case when browser = 'Edge' then 1 end), 0), 2) as credit_limit_per_edge,
    count_if(country = 'TH') as th_events,
    sort_array(collect_set(browser)) as browser_list
from enriched
group by browser
having count(case when browser = 'Edge' then 1 end) > 1;
