select location, max(budget) as max_budget from departments where budget between 3144000 and 3887000 group by location having max(budget) > 3525000;
