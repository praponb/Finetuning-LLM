SELECT brand, SUM(cost) total_cost, SUM(SUM(cost)) OVER (ORDER BY brand) AS cumulative_cost FROM products WHERE supplier_id = 14 GROUP BY brand ORDER BY total_cost;
