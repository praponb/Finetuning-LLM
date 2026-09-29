select *, rank() over (partition by customer_id, status order by opened_date nulls last) as dedup_rank from accounts qualify dedup_rank = 1;
