WITH scored AS (
    SELECT
        id,
        customer_id,
        end_date,
        status,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan, status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY start_date DESC, id) event_seq,
        PERCENT_RANK() OVER (PARTITION BY MONTH(start_date), customer_id ORDER BY monthly_fee DESC NULLS LAST) monthly_fee_percent_rank,
        LEAD(monthly_fee, 1, 0) OVER (PARTITION BY DATE_TRUNC('week', start_date), status ORDER BY start_date, id) next_monthly_fee
    FROM subscriptions
),
deduped AS (
    SELECT
        id,
        customer_id,
        end_date,
        last_known_monthly_fee,
        event_seq,
        monthly_fee_percent_rank,
        next_monthly_fee
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee_percent_rank NULLS LAST, id) = 1
),
scored_subscriptions AS (
    SELECT
        id,
        customer_id,
        end_date,
        last_known_monthly_fee,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY customer_id), 4) event_seq_share,
        SUM(last_known_monthly_fee) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), customer_id ORDER BY last_known_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last_known_monthly_fee,
        NTILE(4) OVER (ORDER BY event_seq, id) event_seq_bucket,
        LAST_VALUE(next_monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY last_known_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_next_monthly_fee
    FROM deduped
),
joined AS (
    SELECT
        s.id,
        s.customer_id,
        s.end_date,
        s.last_known_monthly_fee,
        s.last_known_next_monthly_fee,
        c.city,
        c.country
    FROM scored_subscriptions s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
stats AS (
    SELECT
        CASE WHEN last_known_next_monthly_fee >= 232 THEN 'HIGH' ELSE 'LOW' END AS last_known_next_monthly_fee_tier,
        APPROX_PERCENTILE(last_known_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS last_known_monthly_fee_quartiles,
        COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
        SIZE(COLLECT_SET(country)) AS num_countries
    FROM joined
    GROUP BY CASE WHEN last_known_next_monthly_fee >= 232 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    last_known_next_monthly_fee_tier,
    last_known_monthly_fee_quartiles,
    sg_count,
    num_countries
FROM stats
ORDER BY num_countries DESC NULLS FIRST;
