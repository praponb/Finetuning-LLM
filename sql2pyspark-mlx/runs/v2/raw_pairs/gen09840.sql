SELECT s.id, s.city FROM stores s WHERE s.opened_date > '2025-10-02' AND s.id IN (SELECT store_id FROM orders WHERE shipping_fee BETWEEN 27 AND 38) ORDER BY id DESC LIMIT 10
