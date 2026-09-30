from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.round("budget", 2).alias("budget_rounded"),
        F.round("budget", 1).alias("budget_rounded_2"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

result = (
    base
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Bangkok", "New York"])
    .agg(F.sum("budget_rounded_2"))
    .select(
        "dept_name",
        F.col("Tokyo").alias("tokyo"),
        F.col("Bangkok").alias("bangkok"),
        F.col("New York").alias("new_york"),
    )
)
