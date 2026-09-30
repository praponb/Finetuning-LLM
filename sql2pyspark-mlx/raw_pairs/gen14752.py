from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "budget",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
