SELECT status FROM payments WHERE method IN ('promptpay', 'cash') INTERSECT SELECT status FROM shipments;
