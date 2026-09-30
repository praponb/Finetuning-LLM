select case when rating >= 4.5 then 'high' when rating >= 2.2 then 'mid' else 'low' end rating_bucket, round(stddev(rating), 2) stddev_rating, sort_array(collect_set(city)) city_list
from suppliers
where country in ('UK', 'TH') or country in ('JP', 'US')
group by rating_bucket
order by rating_bucket desc, stddev_rating desc
limit 20;
