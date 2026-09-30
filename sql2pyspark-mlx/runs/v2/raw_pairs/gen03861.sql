with Ranked as (
    select *, rank() over (partition by carrier order by cost) as rn
    from shipments
)
select *
from Ranked
where rn <= 2
