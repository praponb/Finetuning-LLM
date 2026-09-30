with joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.start_date,
        src.end_date,
        b.age
    from subscriptions src
    left join customers b
        on src.customer_id = b.id
)
-- Grouped metrics
select
    date_trunc('week', start_date) as week_start,
    case when age >= 66 then 'HIGH' else 'LOW' end as age_tier,
    customer_id,
    count(*) as cnt,
    count(*) as num_rows,
    count(*) as cnt_2
from joined
group by date_trunc('week', start_date), case when age >= 66 then 'HIGH' else 'LOW' end, customer_id
having cnt_2 > 1
order by cnt desc
