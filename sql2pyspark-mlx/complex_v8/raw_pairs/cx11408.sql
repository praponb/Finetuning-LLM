WITH valid AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
      AND premium < 3521.8
)
SELECT 'policy' AS source, id AS key_id, holder_id AS ref_id
FROM valid
UNION
SELECT 'adjuster' AS source, id AS key_id, level AS ref_id
FROM adjusters
ORDER BY key_id, source
