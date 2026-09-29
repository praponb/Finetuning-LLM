with scored as (
    select
        id,
        supplier_name,
        country,
        city,
        sum(rating) over (partition by city order by rating, id rows between 6 preceding and current row) as last7_sum_rating,
        max(rating) over (partition by city order by rating, id rows between 6 preceding and current row) as last7_max_rating,
        count(rating) over (partition by city order by rating, id rows between 6 preceding and current row) as last7_count_rating,
        rank() over (partition by country order by rating desc) as rating_rank
    from suppliers
),
ranked as (
    select *
    from scored
    qualify rank() over (partition by city order by last7_max_rating) = 1
),
latest_match as (
    select
        s.id,
        s.last7_max_rating,
        s.rating_rank,
        top1.product_price
    from ranked s
    left join lateral (
            select c.price as product_price
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from latest_match s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.category = 'Fashion'
        )
),
with_windows as (
    select
        *,
        sum(rating_rank) over (order by rating_rank, id rows between unbounded preceding and current row) as cumulative_rating_rank,
        sum(rating_rank) over w_ordered as running_rating_rank,
        avg(product_price) over (order by product_price, id rows between 6 preceding and current row) as product_price_avg_6,
        sum(product_price) over w_ordered as running_product_price
    from not_flagged
    where product_price >= 1130
    window w_ordered as (order by product_price, id rows between unbounded preceding and current row)
),
scored_suppliers as (
    select
        *,
        cume_dist() over (order by cumulative_rating_rank) as cumulative_rating_rank_cume_dist,
        avg(product_price) over (order by product_price, id rows between 3 preceding and 1 preceding) as product_price_avg_3_prior,
        last_value(last7_max_rating, true) over (order by cumulative_rating_rank, id rows between unbounded preceding and current row) as last_known_last7_max_rating,
        lead(rating_rank, 1) over (order by rating_rank, id) as next_rating_rank
    from with_windows
)
select
    id,
    last7_max_rating,
    rating_rank,
    product_price,
    cumulative_rating_rank,
    running_rating_rank,
    running_product_price,
    cumulative_rating_rank_cume_dist,
    product_price_avg_3_prior
from scored_suppliers;
