from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_product_premium_tier_by_star = Window.partitionBy("product", "premium_tier").orderBy(F.col("start_date").desc(), "id")

cleaned_policys = (
    df_policies
    .filter((F.col("premium") >= 1541.8) & (F.col("end_date") >= F.lit("2026-01-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.when(F.col("premium") >= 3817.5, "HIGH").when(F.col("premium") >= 2033.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

metrics = (
    cleaned_policys
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "premium_tier",
        F.lead("start_date").over(w_all_by_premium).alias("next_start_date"),
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.dense_rank().over(w_all_by_premium).alias("premium_dense_rank"),
        F.lag("premium_filled", 2, 0).over(w_product_premium_tier_by_star).alias("prev2_premium_filled"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

rollup_base = (
    filtered
    .groupBy("product")
    .agg(
        F.sum("premium").alias("total_premium"),
        F.count("*").alias("cnt"),
        F.max("premium").alias("max_premium_check"),
    )
    .filter(F.col("max_premium_check") > 1051.9)
    .drop("max_premium_check")
)

result = (
    rollup_base
    .filter(F.col("product").isin("HEALTH", "TRAVEL"))
    .select("product", "total_premium", "cnt")
    .orderBy(F.col("product").asc_nulls_last(), F.col("cnt").desc_nulls_first())
)
