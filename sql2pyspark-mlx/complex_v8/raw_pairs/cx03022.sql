WITH prepared AS (
    -- Filter tariff_windows
    SELECT *
    FROM tariff_windows
    WHERE rate > 3.0
      AND tariff = 'FLAT'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ROUND(rate - AVG(rate) OVER w_tariff, 2) AS rate_vs_tariff_avg,
        ROW_NUMBER() OVER (ORDER BY rate DESC, id) AS seq_no
    FROM prepared
    WINDOW w_tariff AS (PARTITION BY tariff)
),
grouped AS (
    -- Summarise per group
    SELECT
        seq_no,
        MIN(valid_to) AS first_valid_to,
        MAX(valid_to) AS last_valid_to,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    GROUP BY seq_no
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        seq_no,
        first_valid_to,
        last_valid_to,
        distinct_ids,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER (PARTITION BY seq_no, TO_DATE(first_valid_to)), 0), 4) AS seq_no_share_of_seq_no_first_valid_to_to_date,
        SUM(seq_no) OVER (PARTITION BY seq_no, TO_DATE(first_valid_to)) AS seq_no_first_valid_to_to_date_sum_seq_no
    FROM grouped
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN seq_no >= 9 THEN 'large' WHEN seq_no >= 4 THEN 'medium' ELSE 'small' END AS seq_no_tier,
        seq_no - distinct_ids AS seq_no_minus_distinct_ids,
        ROUND(seq_no / NULLIF(distinct_ids, 0), 3) AS seq_no_per_distinct_ids
    FROM sequenced
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        seq_no,
        seq_no_share_of_seq_no_first_valid_to_to_date,
        seq_no_first_valid_to_to_date_sum_seq_no,
        seq_no_minus_distinct_ids,
        MIN(distinct_ids) OVER (PARTITION BY distinct_ids, seq_no_tier) AS distinct_ids_seq_no_tier_min_distinct_ids,
        LAST_VALUE(last_valid_to) OVER (ORDER BY first_valid_to DESC, seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_valid_to,
        COUNT(*) OVER (PARTITION BY distinct_ids, seq_no_tier) AS rows_in_distinct_ids_seq_no_tier,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY distinct_ids, seq_no_tier), 2) AS seq_no_vs_distinct_ids_seq_no_tier_avg
    FROM labeled
),
derived AS (
    -- Add calculated columns
    SELECT
        seq_no,
        seq_no_share_of_seq_no_first_valid_to_to_date,
        seq_no_first_valid_to_to_date_sum_seq_no,
        seq_no_minus_distinct_ids,
        distinct_ids_seq_no_tier_min_distinct_ids,
        seq_no_vs_distinct_ids_seq_no_tier_avg,
        CASE WHEN seq_no >= 6 THEN 'large' WHEN seq_no >= 3 THEN 'medium' ELSE 'small' END AS seq_no_tier,
        FLOOR(seq_no / 2) * 2 AS seq_no_bucket,
        ABS(seq_no - 7) AS seq_no_distance
    FROM with_trends
)
SELECT
    seq_no,
    seq_no_share_of_seq_no_first_valid_to_to_date,
    seq_no_first_valid_to_to_date_sum_seq_no,
    distinct_ids_seq_no_tier_min_distinct_ids,
    seq_no_vs_distinct_ids_seq_no_tier_avg,
    seq_no_bucket,
    seq_no_distance,
    LENGTH(TRIM(seq_no_tier)) AS seq_no_tier_len
FROM derived
