select o.id, o.channel from orders o where o.store_id in (select id from stores where city = 'Bangkok') order by o.id desc, o.channel desc;
