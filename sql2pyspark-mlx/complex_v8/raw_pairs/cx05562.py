from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("status") != "CANCELLED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

qualified = (
    scoped
    .crossJoin(scoped.agg(F.max("premium").alias("overall_max_premium")))
    .filter(F.col("premium") < F.col("overall_max_premium"))
    .drop("overall_max_premium")
)

result = (
    qualified
    .groupBy("end_date")
    .pivot("product", ["HOME", "TRAVEL", "HEALTH"])
    .agg(F.avg("premium"))
    .orderBy("end_date")
)
