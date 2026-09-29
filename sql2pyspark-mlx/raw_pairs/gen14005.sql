with lines as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        element
    from price_tiers
    lateral view explode(tier_limits) t as element
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by currency order by element desc nulls last) as element_dense_rank,
        count(element) over (partition by currency) as currency_count_element
    from lines
),
scored_price_tiers as (
    -- Running and moving aggregates
    select
        *,
        round(element_dense_rank / sum(element_dense_rank) over (partition by currency), 4) as element_dense_rank_share,
        sum(element) over (partition by currency) as currency_sum_element
    from scored
    where currency in ('THB', 'USD', 'EUR')
),
filtered as (
    -- Basic cleansing and defaults
    select *, ceil(element_dense_rank_share) as element_dense_rank_share_whole
    from scored_price_tiers
    where element_dense_rank = 3
)
-- Aggregate with conditional metrics
select
    currency,
    max(element) filter (where currency = 'EUR') as eur_max_element,
    sum(case when currency = 'USD' then element_dense_rank_share_whole else 0 end) as usd_element_dense_rank_share_whole,
    sort_array(collect_set(currency)) as currency_list,
    min(element_dense_rank_share) as min_element_dense_rank_share
from filtered
group by currency;
