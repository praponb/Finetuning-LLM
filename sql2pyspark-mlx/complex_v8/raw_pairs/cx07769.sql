WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        YEAR(start_date) AS start_date_year,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE end_date BETWEEN DATE '2025-06-08' AND DATE '2026-11-25'
      AND product = 'TRAVEL'
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
      AND s.days_start_date_to_end_date <= 350
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        start_date_year,
        days_start_date_to_end_date,
        ROUND(premium / NULLIF(start_date_year, 0), 3) AS premium_per_start_date_year,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM without_matches
),
latest_per_key AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        start_date_year,
        days_start_date_to_end_date,
        premium_per_start_date_year,
        product_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
without_matches_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type = 'theft'
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        premium,
        end_date,
        status,
        start_date_year,
        days_start_date_to_end_date,
        premium_per_start_date_year,
        product_slug,
        ROUND(days_start_date_to_end_date / NULLIF(SUM(days_start_date_to_end_date) OVER (PARTITION BY start_date_year, DATE_TRUNC('MONTH', start_date)), 0), 4) AS days_start_date_to_end_date_share_of_start_date_,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY start_date_year, DATE_TRUNC('MONTH', start_date))) AS distinct_status_in_start_date_year_start_date_mo,
        FIRST_VALUE(days_start_date_to_end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_start_date_to_end_date,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY start_date_year, DATE_TRUNC('MONTH', start_date))) AS distinct_holder_id_in_start_date_year_start_date,
        LAG(premium_per_start_date_year, 2) OVER (PARTITION BY holder_id ORDER BY premium DESC, id) AS prev2_premium_per_start_date_year
    FROM without_matches_2
    WINDOW w_start_date_year_start_date_m AS (PARTITION BY start_date_year, DATE_TRUNC('MONTH', start_date))
)
-- Final projection
SELECT
    id,
    premium,
    end_date,
    status,
    start_date_year,
    days_start_date_to_end_date,
    premium_per_start_date_year,
    product_slug,
    days_start_date_to_end_date_share_of_start_date_,
    distinct_status_in_start_date_year_start_date_mo,
    first_known_days_start_date_to_end_date,
    distinct_holder_id_in_start_date_year_start_date,
    prev2_premium_per_start_date_year,
    DATE_ADD(end_date, 90) AS end_date_plus_90d,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM windowed
