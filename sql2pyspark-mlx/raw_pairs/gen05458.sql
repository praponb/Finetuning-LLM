with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        opened_date,
        cast(sqft as double) as sqft_str,
        trim(city) as clean_city
    from stores
    where opened_date > to_date('2023-10-02')
      and opened_date between '2025-07-05' and '2025-08-01'
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.revenue,
        latest.store_sale_sale_date
    from cleaned s
    left join lateral (
            select c.revenue, c.sale_date as store_sale_sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) latest on true
),
filtered as (
    -- Keep recent, valid rows
    select *, datediff(opened_date, store_sale_sale_date) as days_store_sale_sale_date_to_opened_date
    from with_last
    where year(opened_date) = 2023
),
kept_keys as (
    -- Set difference on keys
    select id
    from filtered
    except distinct
    select id
    from stores
    where store_name is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join kept_keys k on s.id = k.id
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept_rows s
    where exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.order_date <= s.store_sale_sale_date
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.last_restock >= s.opened_date
        )
),
not_flagged_stores as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept s
    where exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.status != 'PENDING'
        )
)
select
    id,
    opened_date,
    sqft_str,
    clean_city,
    store_sale_sale_date,
    days_store_sale_sale_date_to_opened_date
from not_flagged_stores
order by id asc nulls first, days_store_sale_sale_date_to_opened_date asc;
