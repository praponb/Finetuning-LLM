from pyspark.sql import functions as F

src = df_departments.filter(F.col("dept_name") == "Legal")

agg = (
    src
    .groupBy(
        F.when(F.col("location").isin(["Tokyo", "Berlin"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
)

result = agg.filter(F.col("budget_range") >= 54)
