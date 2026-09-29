select end_date from subscriptions except select last_restock from inventory where month(last_restock) in (4, 7, 8) order by end_date desc;
