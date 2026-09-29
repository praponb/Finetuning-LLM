WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        ends_on,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        LOWER(promo_code) AS promo_code_lower,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE starts_on >= '2024-02-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY days_since_ends_on ASC, id ASC) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        ends_on,
        promo_code_lower,
        resolved_channel
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY row_num DESC) AS row_num_2
        FROM deduped
    ) x
    WHERE row_num_2 = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code_lower,
        resolved_channel,
        ROW_NUMBER() OVER (ORDER BY ends_on DESC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY ends_on ASC, id ASC) AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY ends_on ASC, id ASC) AS row_seq
    FROM deduped_promotions
    WHERE ends_on >= '2025-07-06'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        resolved_channel,
        seq_no,
        seq_no_2,
        row_seq,
        row_seq * seq_no_2 AS row_seq_times_seq_no_2
    FROM metrics
    WHERE row_seq > 3
)
SELECT
    id,
    seq_no,
    seq_no_2,
    row_seq
FROM cleaned
ORDER BY seq_no_2 DESC;
