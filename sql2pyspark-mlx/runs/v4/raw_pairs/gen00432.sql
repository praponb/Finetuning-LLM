SELECT s.region, s.opened_date, i.last_restock, i.store_id, i.quantity FROM stores s JOIN inventory i ON i.store_id = s.id WHERE s.opened_date >= '2025-02-01' ORDER BY store_id DESC;
