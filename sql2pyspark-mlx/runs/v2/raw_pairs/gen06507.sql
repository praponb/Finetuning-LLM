select id, opened_date from stores where opened_date = (select min(opened_date) from stores) order by 2, id desc;
