WITH filtered AS (
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        DATE_ADD(opened_date, 54) AS opened_date_plus_54d
    FROM accounts
    WHERE opened_date >= DATE '2024-02-20'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date_plus_54d DESC NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    SELECT
        a.*,
        d.name,
        d.credit_limit,
        d.segment
    FROM deduped a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
deduped_accounts AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, segment ORDER BY credit_limit DESC NULLS LAST, id) = 1
),
with_windows AS (
    SELECT
        *,
        AVG(credit_limit) OVER (PARTITION BY customer_id, status ORDER BY opened_date_plus_54d, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY credit_limit, id) AS row_seq
    FROM deduped_accounts
),
joined AS (
    SELECT
        e.*,
        b.country,
        b.city
    FROM with_windows e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
SELECT
    city,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    COUNT(DISTINCT segment) AS distinct_segments,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    APPROX_PERCENTILE(credit_limit, 0.9) AS credit_limit_p90,
    APPROX_PERCENTILE(credit_limit_avg_3_prior, 0.95) AS credit_limit_avg_3_prior_p95
FROM joined
GROUP BY city
HAVING COUNT(DISTINCT segment) < 1
ORDER BY credit_limit_p90 DESC NULLS FIRST
