from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("budget").between(2511000, 3358000) & F.col("dept_name").isin(["HR", "Sales", "Marketing"])))
    .select("location", "budget")
)
