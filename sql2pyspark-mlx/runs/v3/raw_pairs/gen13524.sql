SELECT country, 'supplier' source FROM suppliers WHERE supplier_name IS NULL UNION ALL SELECT country, 'session' source FROM sessions WHERE started_at >= '2025-10-29';
