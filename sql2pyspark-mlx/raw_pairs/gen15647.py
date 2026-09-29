from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .groupBy(
        "dept_name",
        F.when(F.col("dept_name").isin(["Legal", "Engineering"]), "tier_1").otherwise("other").alias("dept_name_group"),
        "location",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
)
