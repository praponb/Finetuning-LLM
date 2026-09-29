SELECT full_name AS label FROM employees WHERE full_name IS NOT NULL INTERSECT SELECT device AS label FROM sessions WHERE device IS NOT NULL ORDER BY label
