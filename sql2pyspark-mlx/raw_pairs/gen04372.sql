with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        slice(tier_limits, 1, 2) as first_two_limits,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        element_at(tier_prices, 1) as entry_price
    from price_tiers
)
-- Running and moving aggregates
select
    id,
    effective_from,
    currency,
    ntile(4) over (partition by currency order by entry_price, id) entry_price_bucket,
    lead(avg_tier_price, 2) over (partition by currency order by avg_tier_price, id) next_avg_tier_price,
    percent_rank() over (partition by currency order by entry_price desc nulls first) entry_price_percent_rank,
    last_value(entry_price, true) over (partition by currency order by effective_from, id rows between unbounded preceding and current row) last_known_entry_price,
    sum(entry_price) over (partition by currency order by entry_price, id rows between unbounded preceding and current row) running_entry_price
from unpacked
where avg_tier_price < 31
