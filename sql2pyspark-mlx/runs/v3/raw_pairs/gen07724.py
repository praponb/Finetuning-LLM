from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("level").isin(["L5", "L3"]))
    .groupBy("level")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)
