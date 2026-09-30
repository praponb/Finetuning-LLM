WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, store_id ORDER BY discount DESC, id) AS rank_in_group
        FROM orders
    ) t
    WHERE rank_in_group = 1
)
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS amount_per_partner,
    PERCENTILE_APPROX(discount, 0.95) AS discount_p95,
    CORR(amount, discount) AS corr_amount_discount,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM deduped
GROUP BY DATE_TRUNC('WEEK', order_date)
ORDER BY discount_p95 DESC NULLS LAST;
