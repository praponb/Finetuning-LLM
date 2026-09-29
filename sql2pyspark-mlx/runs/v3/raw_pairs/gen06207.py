from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .orderBy(F.col("marketing_count").desc(), "dept_name")
    .limit(50)
)
