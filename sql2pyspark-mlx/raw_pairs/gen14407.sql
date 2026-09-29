with latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by promo_code order by ends_on desc, id) as row_num
        from promotions
    ) ranked
    where row_num = 1
),
remaining_keys as (
    -- Set intersection on keys
    select promo_code, id
    from latest
    intersect all
    select promo_code, id
    from promotions
    where discount_pct > 0.13
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join remaining_keys k on s.promo_code = k.promo_code and s.id = k.id
),
scored as (
    -- Ranking and gap features
    select
        promo_code,
        starts_on,
        row_num,
        round(discount_pct / sum(discount_pct) over (partition by channel), 4) discount_pct_share
    from verified
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, row_num - discount_pct_share row_num_minus_discount_pct_share
    from scored
    where starts_on >= to_date('2023-09-07')
)
select
    promo_code,
    discount_pct_share,
    row_num_minus_discount_pct_share
from filtered
