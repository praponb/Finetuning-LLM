select o.status, o.discount, o.store_id, s.store_name, concat_ws('-', o.status, o.channel) as status_channel_key from orders o left join stores s on o.store_id = s.id;
