select * from stores qualify rank() over (partition by city order by sqft desc nulls first) = 1
