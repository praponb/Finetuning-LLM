from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("hire_date") > "2023-07-01") & ((F.col("level") == "L1") | F.col("level").isin(["L1", "L2", "L4"]))))
    .orderBy(F.col("id").desc())
    .limit(10)
)
