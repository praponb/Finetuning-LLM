select *, hour(started_at) as started_at_hour from sessions order by customer_id desc, device;
