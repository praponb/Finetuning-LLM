select s.sqft, o.store_id, o.channel, length(s.store_name) store_name_length from stores s inner join orders o on o.store_id = s.id where o.store_id is null
