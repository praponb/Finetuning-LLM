with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from suppliers s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.stock_qty <> 231
        )
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(rating) over (partition by country, city order by rating, id rows between unbounded preceding and current row) running_count_rating,
        avg(rating) over (partition by country, city order by rating, id rows between unbounded preceding and current row) running_avg_rating,
        max(rating) over (partition by country, city order by rating, id rows between unbounded preceding and current row) running_max_rating,
        sum(rating) over (partition by country, city order by rating, id rows between unbounded preceding and current row) running_sum_rating,
        dense_rank() over (partition by country order by rating desc) rating_dense_rank
    from kept
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.country,
        s.running_count_rating,
        s.running_avg_rating,
        x.product_brand,
        x.stock_qty
    from scored s
    left join lateral (
            select c.brand as product_brand, c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) x on true
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        round(running_avg_rating / sum(running_avg_rating) over (partition by country, product_brand), 4) running_avg_rating_share,
        row_number() over (partition by country, product_brand order by stock_qty desc, id) event_seq
    from latest_match
    where stock_qty < 352
)
-- Window metrics over several different partitions
select
    id,
    country,
    running_count_rating,
    product_brand,
    stock_qty,
    event_seq,
    row_number() over (partition by country order by stock_qty desc, id) event_seq_2,
    lag(running_avg_rating_share, 1) over (partition by product_brand, country order by running_count_rating, id) prev_running_avg_rating_share,
    sum(running_count_rating) over (partition by country order by stock_qty, id rows between unbounded preceding and current row) cumulative_running_count_rating,
    dense_rank() over (partition by product_brand, country order by running_avg_rating_share nulls last) running_avg_rating_share_dense_rank,
    rank() over (partition by country, product_brand order by running_avg_rating desc nulls last) running_avg_rating_rank
from metrics
