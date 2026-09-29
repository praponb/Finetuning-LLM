select *
from (
    select quarter(opened_date) as opened_quarter, count(distinct sqft) as unique_regions
    from stores
    group by quarter(opened_date)
) as sub
where unique_regions < 9
order by opened_quarter desc
limit 1
