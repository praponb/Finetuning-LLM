with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        category,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        upper(brand) as brand_upper,
        coalesce(brand, 'OTHER') as resolved_brand
    from products
    where launch_date > date '2024-01-02'
      and supplier_id is not null
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date < s.launch_date
        )
),
with_dims as (
    -- Attach suppliers attributes
    select
        e.*,
        d.city,
        d.supplier_name
    from kept e
    left join suppliers d
        on e.supplier_id = d.id
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from with_dims
    qualify row_number() over (partition by category order by launch_date desc nulls first, id) = 1
),
with_dims_products as (
    -- Attach suppliers attributes
    select
        s.*,
        p2.supplier_name as supplier_supplier_name,
        p2.country,
        p2.rating
    from deduped s
    left join suppliers p2
        on s.supplier_id = p2.id
)
select
    id,
    category,
    stock_qty,
    launch_date,
    supplier_id,
    brand_upper,
    resolved_brand,
    city,
    supplier_name
from with_dims_products
order by launch_date desc nulls last;
