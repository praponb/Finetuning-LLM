SELECT id, amount, COALESCE(discount, 0) discount_filled, ROUND(discount * 100, 2) discount_adjusted FROM orders WHERE discount >= 0.16 ORDER BY discount_adjusted DESC, id
