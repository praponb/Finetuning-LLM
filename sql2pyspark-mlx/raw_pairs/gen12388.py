from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & F.col("dept_name").isin(["Legal", "HR"]))
    .groupBy(
        "location",
        F.when(F.col("location").isin(["Tokyo", "New York"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
)
