SELECT level AS label FROM employees UNION ALL SELECT carrier AS label FROM shipments WHERE carrier NOT IN ('DHL', 'UPS', 'Kerry') ORDER BY label;
