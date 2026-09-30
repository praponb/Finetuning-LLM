with base as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(supplier_name, 'Unknown') as supplier_name_filled
    from suppliers
    where city is not null
),
cleaned as (
    -- Keep recent, valid rows
    select *, length(country) as country_length
    from base
    where city is not null
      and country = 'TH'
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.rating,
        s.supplier_name_filled,
        latest.product_category,
        latest.product_brand
    from cleaned s
    left join lateral (
            select c.category as product_category, c.brand as product_brand
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) latest on true
),
base_suppliers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        city,
        rating,
        supplier_name_filled,
        product_category,
        product_brand,
        cast(rating as int) as rating_int,
        trim(product_brand) as clean_product_brand,
        coalesce(city, 'NONE') as resolved_city
    from with_last
    where product_category is not null
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            product_brand,
            city,
            rating_int
        from base_suppliers
    ) src
    pivot (
        avg(rating_int) as amount,
        count(1) as cnt
        for city in ('Singapore', 'Tokyo')
    )
)
select
    product_brand,
    Singapore_amount,
    Singapore_cnt,
    Tokyo_amount
from wide
order by Singapore_amount desc, Tokyo_amount desc
