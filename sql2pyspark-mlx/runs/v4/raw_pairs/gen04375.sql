with filtered as (
    select
        *,
        price - stock_qty as price_minus_stock_qty,
        upper(brand) as brand_upper,
        coalesce(brand, 'UNKNOWN') as resolved_brand
    from products
    where launch_date > date '2023-05-21'
      and supplier_id is not null
)
select
    *,
    sum(price) over w_ordered as cumulative_price,
    sum(cost) over (partition by category, brand order by price, id rows between unbounded preceding and current row) as cumulative_cost
from filtered
window w_ordered as (partition by category order by cost, id rows between unbounded preceding and current row);
