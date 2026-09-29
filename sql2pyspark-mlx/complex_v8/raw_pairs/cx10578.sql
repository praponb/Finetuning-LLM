SELECT tier AS name
FROM support_agents
INTERSECT ALL
SELECT team
FROM support_agents
WHERE tier = 'T2'
  AND YEAR(hired_on) = 2024
