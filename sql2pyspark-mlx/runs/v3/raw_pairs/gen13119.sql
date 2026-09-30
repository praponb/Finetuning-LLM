select c.segment, max(s.started_at) as latest_started_at from sessions s left join customers c on c.id = s.customer_id where s.country = 'TH' group by c.segment;
