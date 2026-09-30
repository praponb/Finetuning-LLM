with numbered as (
    select *, row_number() over (partition by status order by shipped_date desc, id) as row_num
    from shipments
)
select *
from numbered
where row_num <= 5
order by shipped_date;
