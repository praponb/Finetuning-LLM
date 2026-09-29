from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .orderBy("l3_salary")
)
