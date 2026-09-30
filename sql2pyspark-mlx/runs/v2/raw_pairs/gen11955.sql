SELECT id, amount, LOWER(status) AS status_lower FROM payments WHERE paid_at < '2025-01-01' OR method IN ('card', 'cash');
