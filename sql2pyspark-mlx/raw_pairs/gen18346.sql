with base as (
    select *, date_sub(sale_date, 19) sale_date_minus_19d
    from store_sales
    where revenue > 1300
),
ranked as (
    select *, row_number() over (partition by store_id, product_id order by units desc nulls last, id) as dedup_rank
    from base
    qualify dedup_rank = 1
),
ranked_store_sales as (
    select
        id,
        store_id,
        product_id
    from (
        select *, row_number() over (partition by product_id order by sale_date_minus_19d, id) as dedup_rank_2
        from ranked
    ) ranked
    where dedup_rank_2 = 1
),
with_dims as (
    select s.*, d.product_name
    from ranked_store_sales s
    left join products d
        on s.product_id = d.id
    where d.product_name like 'Yog%'
),
valid as (
    select *, upper(product_name) product_name_upper
    from with_dims
    where product_name like '%Canc%'
),
joined as (
    select
        a.*,
        d.sqft,
        d.region,
        d.opened_date
    from valid a
    left join stores d
        on a.store_id = d.id
),
with_windows as (
    select
        id,
        store_id,
        product_id,
        product_name_upper,
        sqft,
        opened_date,
        row_number() over (partition by product_id order by sqft, id) event_seq,
        ntile(10) over (partition by product_id order by sqft, id) sqft_bucket
    from joined
),
daily as (
    select
        case when sqft_bucket >= 8 then 'PREMIUM' else 'STANDARD' end as sqft_bucket_tier,
        product_id,
        store_id,
        percentile_approx(sqft, 0.5) as sqft_p50,
        count(*) as num_rows,
        percentile_approx(sqft, 0.5) as sqft_p50_2
    from with_windows
    group by 1, 2, 3
)
select
    sqft_bucket_tier,
    product_id,
    store_id,
    sqft_p50,
    num_rows,
    sqft_p50_2
from daily
order by product_id desc, sqft_p50 desc;
