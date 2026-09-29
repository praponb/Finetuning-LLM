with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from suppliers s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and length(ch.product_name) >= 9
        )
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.price < 490
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        country,
        row_number() over (partition by city order by rating, id) as seq_no,
        row_number() over (partition by country, city order by rating, id) as seq_no_2,
        last_value(city, true) over (partition by country order by rating, id rows between unbounded preceding and current row) as last_known_city
    from not_flagged
),
matched_keys as (
    -- Set difference on keys
    select country, id
    from scored
    except distinct
    select country, id
    from suppliers
    where country != 'JP'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    join matched_keys k on s.country = k.country and s.id = k.id
),
not_flagged_suppliers as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and array_contains(x.tags, 'bestseller')
        )
)
-- Multi-level aggregation
select
    case when seq_no >= 7 then 'LARGE' else 'SMALL' end as seq_no_tier,
    count(*) as num_rows,
    approx_percentile(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles,
    sort_array(collect_set(country)) as country_list,
    sum(seq_no_2) as total_seq_no_2
from not_flagged_suppliers
group by case when seq_no >= 7 then 'LARGE' else 'SMALL' end
having num_rows >= 1
order by seq_no_tier nulls first
