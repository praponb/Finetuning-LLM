select opened_date, 'account' source from accounts union select last_restock, 'inventory' source from inventory where quantity <> 136 order by source desc
