with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        region,
        opened_date,
        sqft,
        cast(sqft as string) as sqft_str,
        coalesce(region, 'OTHER') as region_filled
    from stores
    where opened_date > to_date('2023-04-18')
      and region in ('West', 'North')
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date < s.opened_date
        )
),
filtered_stores as (
    -- Basic cleansing and defaults
    select
        id,
        region,
        sqft,
        sqft_str,
        add_months(opened_date, 12) as opened_date_plus_months
    from kept
    where opened_date > date '2023-02-28'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(sqft) over (partition by region order by sqft, id) as prev_sqft,
        lead(sqft) over (partition by region order by sqft, id) as next_sqft,
        row_number() over (partition by region order by sqft, id) as row_seq,
        row_number() over (order by opened_date_plus_months, id) as seq_no
    from filtered_stores
    where sqft between 4701 and 16031
),
latest_match as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, x.store_sale_sale_date
    from metrics s
    left join lateral (
            select c.sale_date as store_sale_sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    region,
    sqft,
    sqft_str,
    next_sqft,
    seq_no
from latest_match
order by seq_no nulls first, id;
