with unpacked as (
    -- Semi-structured parsing
    select
        id,
        currency,
        tier_prices,
        discounts,
        array_min(tier_prices) as lowest_price,
        slice(tier_limits, 1, 2) as first_two_limits,
        aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price
    from price_tiers
)
-- Running and moving aggregates
select
    id,
    lowest_price,
    avg_tier_price,
    lead(lowest_price) over w_hist as next_lowest_price,
    lead(avg_tier_price) over w_hist as next_avg_tier_price,
    row_number() over w_hist as seq_no,
    row_number() over (partition by currency order by lowest_price desc, id) as event_seq
from unpacked
window w_hist as (partition by currency order by lowest_price, id);
