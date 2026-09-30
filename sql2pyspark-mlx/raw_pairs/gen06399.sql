with metrics_long as (
    select *
    from (
        select
            id,
            supplier_id,
            cost,
            price
        from products
    ) src
    unpivot include nulls (
        value for measure in (cost, price)
    )
)
select
    case when value >= 63 then 'LARGE' else 'SMALL' end as value_tier,
    sum(case when measure = 'cost' then value else 0 end) as cost_value,
    approx_percentile(value, array(0.25, 0.5, 0.75)) as value_quartiles
from metrics_long
group by case when value >= 63 then 'LARGE' else 'SMALL' end
order by value_tier desc;
