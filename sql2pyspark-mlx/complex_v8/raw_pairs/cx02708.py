from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-08-27").cast("date")) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket_2"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.year(F.col("p.joined_on")) == 2024), "left_anti")
)

result = (
    qualified
    .groupBy("holder_id", "product")
    .pivot("status", ["LAPSED", "ACTIVE", "CANCELLED"])
    .agg(F.max("premium").alias("max_v"), F.count("premium").alias("n"))
)
