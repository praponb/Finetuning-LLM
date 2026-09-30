from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Singapore", "Berlin"]))
    .select(
        "id",
        "location",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.round((F.col("budget") * 0.9), 2).alias("budget_scaled"),
    )
)
