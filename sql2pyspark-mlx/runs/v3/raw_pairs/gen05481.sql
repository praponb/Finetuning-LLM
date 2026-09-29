SELECT id, customer_id, device FROM sessions WHERE device <> 'tablet' AND country IN ('JP', 'DE', 'TH') ORDER BY customer_id DESC, device;
