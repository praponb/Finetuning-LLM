with metrics as (
    select
        *,
        row_number() over w_hist as event_seq,
        row_number() over w_recent as event_seq_2,
        percent_rank() over (partition by city order by sqft desc nulls first) as sqft_percent_rank,
        sum(sqft) over (partition by date_trunc('YEAR', opened_date), region order by opened_date, id rows between unbounded preceding and current row) as cumulative_sqft
    from stores
    window w_hist as (partition by city order by opened_date desc, id), w_recent as (order by opened_date, id)
),
latest as (
    select *, row_number() over (partition by city order by event_seq, id) as row_num
    from metrics
    qualify row_num = 1
),
enriched as (
    select
        id,
        city,
        opened_date,
        sqft,
        event_seq_2,
        sqft_percent_rank,
        cumulative_sqft,
        row_num,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share,
        first_value(sqft) ignore nulls over (partition by region, city order by sqft, id rows between unbounded preceding and current row) as first_known_sqft
    from latest
),
latest_stores as (
    select *, rank() over (partition by city order by event_seq_2 nulls first) as latest_rank
    from enriched
    qualify latest_rank = 1
),
deduped as (
    select *, row_number() over (partition by city order by latest_rank desc, id) as row_num_2
    from latest_stores
    qualify row_num_2 = 1
),
kept as (
    select s.*
    from deduped s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date >= s.opened_date
        )
)
select
    id,
    city,
    sqft,
    event_seq_2,
    row_num,
    latest_rank,
    row_num_2,
    case when city rlike '^(Bangkok|Berlin)$' then 'OTHER_ZONE' when city rlike '^(Chiang Mai|New York)$' then 'PRIORITY' else 'GROUP_B' end as city_zone
from kept
order by id nulls first, city desc
limit 10
