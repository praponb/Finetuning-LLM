WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.element,
        b.country,
        b.signup_date,
        b.age
    FROM flattened src
    JOIN customers b
        ON src.customer_id <=> b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.name,
        d.signup_date AS customer_signup_date
    FROM enriched e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(age) OVER w_part AS customer_id_count_age,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share
    FROM with_dims
    WINDOW w_part AS (PARTITION BY customer_id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(age_share) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_age_share,
        RANK() OVER (ORDER BY age_share DESC) AS age_share_rank,
        ROUND(customer_id_count_age / SUM(customer_id_count_age) OVER (PARTITION BY country), 4) AS customer_id_count_age_share,
        MIN(age) OVER (PARTITION BY element) AS element_min_age
    FROM with_windows
    WHERE name LIKE 'F%'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        element,
        country,
        signup_date,
        age,
        name,
        customer_signup_date,
        customer_id_count_age,
        age_share,
        customer_id_stddev_samp_age_share,
        element_min_age,
        age_share_rank * customer_id_stddev_samp_age_share AS age_share_rank_times_customer_id_stddev_samp_age_share
    FROM scored
    WHERE name LIKE '%Lee%'
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.country AS customer_country,
        p.segment
    FROM base src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
-- Multi-level aggregation
SELECT
    segment,
    ROUND(SUM(customer_id_count_age) / NULLIF(COUNT(CASE WHEN element = 'WELCOME5' THEN 1 END), 0), 2) AS customer_id_count_age_per_welcome5,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
    SORT_ARRAY(COLLECT_SET(element)) AS element_list,
    AVG(age_share_rank_times_customer_id_stddev_samp_age_share) FILTER (WHERE country = 'DE') AS de_avg_age_share_rank_times_customer_id_stddev_samp_age_share
FROM with_dims_carts
GROUP BY 1;
