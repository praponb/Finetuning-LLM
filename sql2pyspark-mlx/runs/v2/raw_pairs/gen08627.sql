select s.id, s.region, s.opened_date from stores s where s.id in (select store_id from inventory where quantity between 71 and 104);
