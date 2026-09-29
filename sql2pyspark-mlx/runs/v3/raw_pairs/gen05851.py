from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Sales"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .filter(F.col("avg_budget") > 62)
    .orderBy(F.col("avg_budget").desc())
)
