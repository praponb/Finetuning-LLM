from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("location").isin(["Berlin", "New York"]) & (F.col("location") == "Chiang Mai") & ~F.col("location").isin(["Berlin", "London", "Chiang Mai"])))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
