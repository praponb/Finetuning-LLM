select sum(case when country = 'TH' then rating else 0 end) as th_rating from suppliers;
