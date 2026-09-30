SELECT SUM(CASE WHEN status = 'LOST' THEN cost ELSE 0 END) lost_cost, COUNT(*) cnt
FROM shipments;
