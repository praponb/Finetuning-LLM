with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        val
    from price_tiers
    lateral view explode(discounts) e as val
),
filtered as (
    -- Keep recent, valid rows
    select *, date_sub(effective_from, 72) as effective_from_minus_72d
    from lines
    where effective_from >= to_date('2024-06-05')
),
cleaned as (
    -- Keep recent, valid rows
    select *, round(datediff(effective_from_minus_72d, effective_from) / 30, 1) as months_effective_from_to_effective_from_minus_72d
    from filtered
    where effective_from >= date '2024-03-19'
      and val between 30 and 43
),
scored as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (order by months_effective_from_to_effective_from_minus_72d asc nulls last) as months_effective_from_to_effective_from_minus_72d_cume_dist,
        rank() over w_hist as months_effective_from_to_effective_from_minus_72d_rank
    from cleaned
    window w_hist as (order by months_effective_from_to_effective_from_minus_72d desc nulls first)
),
scored_price_tiers as (
    -- Window metrics over several different partitions
    select
        val,
        months_effective_from_to_effective_from_minus_72d,
        months_effective_from_to_effective_from_minus_72d_cume_dist,
        sum(months_effective_from_to_effective_from_minus_72d) over (partition by year(effective_from_minus_72d)) as effective_from_minus_72d_year_sum_months_effective_from_to_effective_from_minus_72d,
        percent_rank() over (order by val asc) as val_percent_rank
    from scored
)
select
    val,
    months_effective_from_to_effective_from_minus_72d,
    months_effective_from_to_effective_from_minus_72d_cume_dist,
    val_percent_rank
from scored_price_tiers
order by months_effective_from_to_effective_from_minus_72d desc
