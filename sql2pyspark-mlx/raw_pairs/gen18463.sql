with latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by store_id order by units desc, id) as rank_in_group
    from store_sales
    qualify rank_in_group = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by store_id, product_id order by sale_date desc nulls last, id) as rank_in_group_2
    from latest
    qualify rank_in_group_2 = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_id,
        revenue,
        rank_in_group,
        rank_in_group_2,
        first_value(units) ignore nulls over (partition by product_id order by sale_date, id rows between unbounded preceding and current row) as first_known_units,
        ntile(4) over (partition by store_id order by units desc, id) as units_bucket,
        percent_rank() over (partition by product_id order by units desc) as units_percent_rank
    from deduped
    where rank_in_group_2 >= 1
),
with_dims as (
    -- Attach products attributes
    select
        src.*,
        d.brand,
        d.price
    from scored src
    left join products d
        on src.product_id = d.id
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_id,
        rank_in_group,
        rank_in_group_2,
        first_known_units,
        units_percent_rank,
        price,
        first_value(first_known_units, true) over (partition by product_id, brand order by revenue, id rows between unbounded preceding and current row) as first_known_first_known_units,
        round(first_known_units / sum(first_known_units) over (partition by product_id), 4) as first_known_units_share
    from with_dims
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    product_id,
    measure,
    val
from (
    select
        id,
        product_id,
        units_percent_rank,
        first_known_units_share,
        price
    from with_windows
) src
unpivot (
    val for measure in (units_percent_rank as UNITS_PERCENT_RANK, first_known_units_share as FIRST_KNOWN_UNITS_SHARE, price)
) u
