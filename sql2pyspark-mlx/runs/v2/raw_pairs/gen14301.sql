select launch_year, books_count, pct_home
from (
    select year(launch_date) as launch_year, count(case when category = 'Books' then 1 end) as books_count, round((100 * sum(case when category = 'Home' then 1 else 0 end)) / count(*), 1) as pct_home
    from products
    where cost between 500 and 590
    group by year(launch_date)
) as agg
where pct_home < 42
order by books_count desc, pct_home desc
