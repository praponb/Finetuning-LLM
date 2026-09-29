SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS pos FROM payments) AS ranked WHERE pos <= 1;
