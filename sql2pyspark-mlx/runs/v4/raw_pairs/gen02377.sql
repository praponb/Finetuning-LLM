with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from stores s
    where not exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.last_restock <= s.opened_date
        )
)
-- Multi-level aggregation
select
    date_trunc('month', opened_date) as activity_month,
    avg(sqft) as avg_sqft,
    count(*) as row_count,
    round(avg(sqft), 2) as avg_sqft_2,
    bit_or(sqft) as sqft_bit_or,
    count(*) as cnt,
    size(collect_set(region)) as num_regions
from kept
group by date_trunc('month', opened_date)
order by avg_sqft desc;
