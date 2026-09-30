SELECT id,
       CASE WHEN rating >= 4.5 THEN 'A'
            WHEN rating >= 3.5 THEN 'B'
            WHEN rating >= 2.5 THEN 'C'
            ELSE 'D' END AS grade
FROM suppliers;
