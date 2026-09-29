from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "budget",
        "dept_name",
        F.when(F.col("location").isin(["Chiang Mai", "Berlin"]), "Group A").otherwise("Other").alias("location_group"),
        F.round((F.col("budget") * 1.15), 2).alias("adjusted_budget"),
    )
)
