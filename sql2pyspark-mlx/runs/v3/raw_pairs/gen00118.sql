select *, trim(device) as clean_device from sessions where not (customer_id in (34, 36, 39) and customer_id = 36);
