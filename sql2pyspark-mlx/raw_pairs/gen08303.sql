with enriched as (
    -- Ranking and gap features
    select
        *,
        first_value(city, true) over (order by rating, id rows between unbounded preceding and current row) first_known_city,
        dense_rank() over (partition by country, city order by rating) rating_dense_rank,
        last_value(rating) ignore nulls over w_recent last_known_rating
    from suppliers
    window w_recent as (partition by city order by rating, id rows between unbounded preceding and current row)
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        supplier_name,
        country,
        rating_dense_rank
    from (
        select *, row_number() over (partition by city order by last_known_rating desc, id) as row_num
        from enriched
    ) x
    where row_num = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(rating_dense_rank) over (partition by country order by rating_dense_rank, id rows between unbounded preceding and current row) running_min_rating_dense_rank,
        count(rating_dense_rank) over (partition by country order by rating_dense_rank, id rows between unbounded preceding and current row) running_count_rating_dense_rank,
        max(rating_dense_rank) over (partition by country order by rating_dense_rank, id rows between unbounded preceding and current row) running_max_rating_dense_rank
    from latest
),
metrics_suppliers as (
    -- Running and moving aggregates
    select
        *,
        avg(running_max_rating_dense_rank) over (partition by country order by rating_dense_rank, id rows between 3 preceding and 1 preceding) running_max_rating_dense_rank_avg_3_prior,
        avg(running_min_rating_dense_rank) over (order by running_min_rating_dense_rank, id rows between 3 preceding and current row) running_min_rating_dense_rank_avg_3
    from metrics
    where running_min_rating_dense_rank is not null
)
-- Ranking and gap features
select
    *,
    avg(running_max_rating_dense_rank_avg_3_prior) over (partition by country order by running_max_rating_dense_rank_avg_3_prior, id rows between 3 preceding and 1 preceding) running_max_rating_dense_rank_avg_3_prior_avg_3_prior,
    row_number() over (order by running_min_rating_dense_rank_avg_3, id) event_seq,
    row_number() over w_recent seq_no,
    stddev_samp(running_count_rating_dense_rank) over w_part country_stddev_samp_running_count_rating_dense_rank,
    lag(running_max_rating_dense_rank_avg_3_prior, 2) over w_ordered prev_running_max_rating_dense_rank_avg_3_prior
from metrics_suppliers
window w_recent as (partition by country order by running_min_rating_dense_rank_avg_3 desc, id), w_part as (partition by country), w_ordered as (partition by country order by rating_dense_rank, id);
