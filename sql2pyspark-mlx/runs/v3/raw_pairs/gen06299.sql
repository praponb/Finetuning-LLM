select budget, case when location = 'Bangkok' then 1 else 0 end as is_bangkok from departments where budget between 1111000 and 1875000 and budget between 1764000 and 3361000
