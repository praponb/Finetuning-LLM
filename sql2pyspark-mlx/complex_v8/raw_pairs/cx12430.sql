WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM policies
    WHERE holder_id <= 19
      AND YEAR(start_date) = 2024
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM prepared
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM prepared s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        end_date_month,
        max_premium,
        premium_minus_max,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium, id) AS premium_seq,
        ROUND(max_premium - AVG(max_premium) OVER w_status_product, 2) AS max_premium_vs_status_product_avg,
        RANK() OVER w_all_by_premium_minus_max_tie AS premium_minus_max_rank,
        DENSE_RANK() OVER w_status_holder_id_by_start_da AS start_date_dense_rank
    FROM compared
    WINDOW w_status_product AS (PARTITION BY status, product), w_all_by_premium_minus_max_tie AS (ORDER BY premium_minus_max NULLS LAST), w_status_holder_id_by_start_da AS (PARTITION BY status, holder_id ORDER BY start_date DESC)
)
SELECT
    id,
    holder_id,
    end_date,
    status,
    end_date_month,
    max_premium,
    premium_minus_max,
    premium_seq,
    max_premium_vs_status_product_avg,
    premium_minus_max_rank,
    GREATEST(start_date_dense_rank, premium_seq) AS greatest_start_date_dense_rank_premium_seq,
    DATE_FORMAT(end_date_month, 'MMM yyyy') AS end_date_month_label
FROM with_windows
