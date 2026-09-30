with filtered_products as (
    select *
    from products
    where brand in ('Initech', 'Globex') and category in ('Electronics', 'Home', 'Grocery') and upper(product_name) like '%OFF%'
),
summary as (
    select category, round((100 * sum(case when category = 'Home' then 1 else 0 end)) / count(*), 1) as pct_home
    from filtered_products
    group by category
)
select *
from summary
where pct_home < 26
order by pct_home desc, category
limit 5;
