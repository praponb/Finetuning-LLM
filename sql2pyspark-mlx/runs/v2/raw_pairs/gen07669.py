from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 3943000) & ((F.col("budget") < 3651000) | (F.col("location") == "Bangkok")))
    .orderBy(F.col("dept_name").desc(), "id")
    .limit(10)
)
