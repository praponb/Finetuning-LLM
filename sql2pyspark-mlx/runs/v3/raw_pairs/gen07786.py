from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["HR", "Legal"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
)
