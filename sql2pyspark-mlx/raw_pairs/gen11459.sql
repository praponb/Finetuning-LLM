SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount NULLS LAST, id) AS dedup_rank FROM payments QUALIFY dedup_rank = 1;
