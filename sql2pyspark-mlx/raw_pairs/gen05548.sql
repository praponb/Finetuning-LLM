SELECT MIN(budget) min_budget FROM departments WHERE budget BETWEEN 2916000 AND 3209000 AND location IN ('Bangkok', 'Berlin') AND location IS NOT NULL;
