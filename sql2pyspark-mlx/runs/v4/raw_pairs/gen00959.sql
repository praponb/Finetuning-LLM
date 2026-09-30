WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
joined AS (
    SELECT
        src.*,
        c.credit_limit,
        c.signup_date
    FROM lines src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
scored AS (
    SELECT
        id,
        customer_id,
        created_at,
        code,
        credit_limit,
        signup_date,
        DENSE_RANK() OVER (PARTITION BY code, customer_id ORDER BY credit_limit) credit_limit_dense_rank,
        SUM(credit_limit) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) credit_limit_sum_2h
    FROM joined
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY customer_id) customer_id_stddev_samp_credit_limit,
        AVG(credit_limit) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) credit_limit_avg_2h,
        PERCENT_RANK() OVER (ORDER BY credit_limit) credit_limit_percent_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id), 4) credit_limit_share
    FROM scored
),
joined_carts AS (
    SELECT
        a.*,
        d.email,
        d.segment,
        d.city
    FROM with_windows a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
joined_carts_2 AS (
    SELECT s.*, c.city AS customer_city
    FROM joined_carts s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
SELECT
    CASE WHEN credit_limit_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_dense_rank_tier,
    code,
    ROUND(SUM(credit_limit_share) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) AS credit_limit_share_per_consumer,
    PERCENTILE_APPROX(credit_limit_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_percent_rank_quartiles,
    ROUND(SUM(credit_limit_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS credit_limit_dense_rank_per_bangkok,
    COUNT(DISTINCT customer_city) AS distinct_customer_cities,
    MAX(credit_limit_sum_2h) FILTER (WHERE customer_city = 'Bangkok') AS bangkok_max_credit_limit_sum_2h
FROM joined_carts_2
GROUP BY CASE WHEN credit_limit_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END, code
HAVING bangkok_max_credit_limit_sum_2h < 0;
