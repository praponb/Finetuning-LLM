select id, region from stores where city != 'Singapore' union all select id, region from stores where region = 'North' order by id;
