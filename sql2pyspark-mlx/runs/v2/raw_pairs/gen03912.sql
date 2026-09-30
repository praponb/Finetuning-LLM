select id, country, customer_id from sessions where device in ('desktop', 'tablet', 'mobile') intersect select id, country, customer_id from sessions where device = 'mobile';
