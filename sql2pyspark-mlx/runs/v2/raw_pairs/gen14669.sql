select id, city, opened_date, row_number() over (partition by city order by opened_date, id) as row_num from stores where store_name like '%Downtown%' order by city, row_num desc;
