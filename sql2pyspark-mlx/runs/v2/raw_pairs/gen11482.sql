select id, segment, age, avg(age) over (partition by segment) as segment_avg_age from customers order by 1 asc
