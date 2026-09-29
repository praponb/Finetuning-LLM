from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
    .orderBy("dept_name")
)
