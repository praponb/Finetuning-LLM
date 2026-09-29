from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Engineering"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.min("budget").alias("min_budget"))
)
