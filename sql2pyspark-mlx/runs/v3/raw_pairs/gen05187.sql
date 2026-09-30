SELECT id, category, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE launch_date BETWEEN '2024-07-01' AND '2025-02-01';
