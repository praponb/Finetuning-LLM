WITH filtered AS (
    SELECT *, CEIL(cost) AS cost_whole
    FROM shipments
    WHERE shipped_date > DATE '2023-10-30'
      AND carrier IS NOT NULL
),
enriched AS (
    SELECT a.*, p.channel
    FROM filtered a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM enriched src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.shipped_date >= pr.starts_on
       AND src.shipped_date < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    CASE WHEN cost >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    APPROX_PERCENTILE(promo_id, ARRAY(0.25, 0.5, 0.75)) AS promo_id_quartiles
FROM matched
GROUP BY CASE WHEN cost >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY cost_tier DESC NULLS FIRST;
