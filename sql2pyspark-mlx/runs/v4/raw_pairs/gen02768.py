from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.countDistinct("salary").alias("unique_levels"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 640000)
    .drop("total_salary")
)
