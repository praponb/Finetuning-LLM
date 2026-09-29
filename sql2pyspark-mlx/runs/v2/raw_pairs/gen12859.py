from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L3", "L4", "L2"]))
    .groupBy("level", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
)
