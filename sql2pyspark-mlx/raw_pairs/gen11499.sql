select id, quantity, case when quantity >= 205 then 'High' when quantity >= 109 then 'Medium' else 'Low' end as quantity_tier from inventory where store_id in (2, 5, 6);
