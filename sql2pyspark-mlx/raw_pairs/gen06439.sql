with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(tier_prices) li as element
),
matched_keys as (
    -- Set difference on keys
    select currency
    from lines
    except
    select currency
    from price_tiers
    where currency = 'USD'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from lines s
    join matched_keys k on s.currency = k.currency
),
scored as (
    -- Ranking and gap features
    select
        id,
        effective_from,
        sum(element) over (partition by currency) as currency_sum_element
    from verified
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(currency_sum_element * 0.9, 2) as adjusted_currency_sum_element,
        cast(currency_sum_element as string) as currency_sum_element_str
    from scored
    where currency_sum_element between 248 and 297
),
filtered as (
    -- Keep recent, valid rows
    select *, cast(adjusted_currency_sum_element as int) as adjusted_currency_sum_element_int
    from valid
    where effective_from > date '2024-06-19'
      and adjusted_currency_sum_element < 47
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        effective_from,
        percent_rank() over (partition by date_trunc('YEAR', effective_from) order by adjusted_currency_sum_element nulls last) as adjusted_currency_sum_element_percent_rank,
        rank() over (order by currency_sum_element nulls last) as currency_sum_element_rank,
        cume_dist() over (order by adjusted_currency_sum_element_int desc) as adjusted_currency_sum_element_int_cume_dist,
        cume_dist() over (order by currency_sum_element desc) as currency_sum_element_cume_dist
    from filtered
    where currency_sum_element <= 231
),
filtered_price_tiers as (
    -- Keep recent, valid rows
    select
        *,
        cast(currency_sum_element_cume_dist as int) as currency_sum_element_cume_dist_int,
        adjusted_currency_sum_element_percent_rank - adjusted_currency_sum_element_int_cume_dist as adjusted_currency_sum_element_percent_rank_minus_adjusted_currency_sum_element_int_cume_dist
    from with_windows
    where effective_from between '2025-02-01' and '2025-02-20'
)
select
    id,
    adjusted_currency_sum_element_int_cume_dist,
    currency_sum_element_cume_dist,
    currency_sum_element_cume_dist_int,
    adjusted_currency_sum_element_percent_rank_minus_adjusted_currency_sum_element_int_cume_dist
from filtered_price_tiers;
