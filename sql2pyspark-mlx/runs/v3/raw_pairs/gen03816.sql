select id, location, budget, length(dept_name) as dept_name_length, floor(budget) as budget_whole from departments where location = 'New York' and budget > 1803000;
