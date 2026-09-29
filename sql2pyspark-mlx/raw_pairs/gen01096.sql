SELECT shipping_fee FROM orders WHERE shipping_fee IS NOT NULL INTERSECT SELECT amount AS shipping_fee FROM payments WHERE amount IS NOT NULL ORDER BY shipping_fee;
