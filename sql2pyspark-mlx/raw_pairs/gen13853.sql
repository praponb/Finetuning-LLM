with lines as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode(tier_prices) as element
    from price_tiers
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(element * 1.15, 2) as element_scaled,
        upper(currency) as currency_upper
    from lines
    where element < 50
),
cleaned_price_tiers as (
    -- Keep recent, valid rows
    select
        id,
        currency,
        element,
        element_scaled,
        element_scaled - element as element_scaled_minus_element
    from cleaned
    where element <= 28
),
cleaned_price_tiers_2 as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(element_scaled_minus_element, 0) as element_scaled_minus_element_filled,
        round(element_scaled_minus_element * 1.15, 2) as element_scaled_minus_element_scaled
    from cleaned_price_tiers
    where element_scaled <= 40
),
cleaned_price_tiers_3 as (
    -- Keep recent, valid rows
    select *, element_scaled_minus_element_scaled - element_scaled as element_scaled_minus_element_scaled_minus_element_scaled
    from cleaned_price_tiers_2
    where element_scaled_minus_element between 39 and 71
),
kept_keys as (
    -- Set difference on keys
    select id, currency
    from cleaned_price_tiers_3
    except all
    select id, currency
    from price_tiers
    where plan = 'enterprise'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned_price_tiers_3 s
    inner join kept_keys k on s.id = k.id and s.currency = k.currency
),
metrics as (
    -- Ranking and gap features
    select
        element,
        element_scaled,
        element_scaled_minus_element,
        element_scaled_minus_element_scaled,
        max(element_scaled_minus_element_scaled_minus_element_scaled) over w_running as currency_max_element_scaled_minus_element_scaled_minus_element_scaled,
        stddev_samp(element) over w_running as currency_stddev_samp_element,
        rank() over (order by element_scaled_minus_element nulls first) as element_scaled_minus_element_rank,
        dense_rank() over (order by element nulls last) as element_dense_rank
    from reconciled
    window w_running as (partition by currency)
),
daily as (
    -- Grouped metrics
    select
        case when currency_stddev_samp_element >= 68 then 'PREMIUM' else 'STANDARD' end as currency_stddev_samp_element_tier,
        case when element_scaled_minus_element >= 67 then 'PREMIUM' else 'STANDARD' end as element_scaled_minus_element_tier,
        round(avg(element), 2) as avg_element,
        approx_percentile(element_dense_rank, array(0.25, 0.5, 0.75)) as element_dense_rank_quartiles,
        percentile_approx(element, array(0.25, 0.5, 0.75)) as element_quartiles,
        percentile_approx(element_scaled_minus_element_scaled, 0.95) as element_scaled_minus_element_scaled_p95,
        grouping_id() as grouping_id
    from metrics
    group by cube (case when currency_stddev_samp_element >= 68 then 'PREMIUM' else 'STANDARD' end, case when element_scaled_minus_element >= 67 then 'PREMIUM' else 'STANDARD' end)
    having percentile_approx(element_scaled_minus_element_scaled, 0.95) < 1000000
)
select
    currency_stddev_samp_element_tier,
    element_scaled_minus_element_tier,
    avg_element,
    element_dense_rank_quartiles,
    element_scaled_minus_element_scaled_p95,
    grouping_id
from daily
order by grouping_id desc
