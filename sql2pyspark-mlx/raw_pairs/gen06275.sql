with cleaned as (
    -- Keep recent, valid rows
    select *, round(rating, 1) as rating_rounded
    from suppliers
    where country != 'UK'
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, x.stock_qty
    from cleaned s
    join lateral (
            select c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and length(c.product_name) > 8
        )
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_rounded,
        stock_qty,
        round(rating_rounded / rating, 2) as rating_rounded_per_rating
    from eligible
    where city is not null
),
cleaned_suppliers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        supplier_name,
        country,
        rating,
        stock_qty,
        rating_rounded_per_rating,
        cast(rating_rounded as int) as rating_rounded_int
    from base
    where rating_rounded_per_rating < 68
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        rating,
        stock_qty,
        rating_rounded_int,
        lead(stock_qty) over w_ordered as next_stock_qty,
        lag(rating) over w_ordered as prev_rating,
        row_number() over w_ordered as seq_no,
        row_number() over w_hist as seq_no_2
    from cleaned_suppliers
    window w_ordered as (partition by country order by stock_qty, id), w_hist as (partition by country order by rating_rounded_int, id)
)
select
    id,
    supplier_name,
    rating,
    stock_qty,
    next_stock_qty,
    prev_rating,
    seq_no,
    case when supplier_name rlike '^(Pacific Wholesale|Tokyo Traders)$' then 'OTHER_ZONE' when supplier_name rlike '^(Siam Parts Co|Global Supply)$' then 'PRIORITY' else 'GROUP_B' end as supplier_name_zone
from with_windows;
