from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("days_since_created_at").desc_nulls_last())
w2 = Window.partitionBy("element").orderBy(F.col("days_since_created_at").asc_nulls_first())
w3 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("element")
w5 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("days_since_created_at_dense_rank_2_tier").orderBy(F.col("spring10_count").desc())

flattened = (
    df_carts
    .select("id", "created_at", F.explode_outer("coupon_codes").alias("element"))
)

valid = (
    flattened
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & (F.to_date("created_at") >= "2025-10-25"))
    .withColumn("element_prefix", F.substring("element", 1, 4))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

filtered = (
    valid
    .filter(F.col("created_at") >= F.lit("2025-03-01 18:00:00").cast("timestamp"))
    .select(
        "id",
        "created_at",
        "element",
        "element_prefix",
        "element_filled",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("days_since_created_at_dense_rank"),
        F.dense_rank().over(w2).alias("days_since_created_at_dense_rank_2"),
        F.percent_rank().over(w).alias("days_since_created_at_percent_rank"),
        F.avg("days_since_created_at").over(w3).alias("days_since_created_at_avg_2h"),
    )
)

scored = (
    enriched
    .withColumn("element_min_days_since_created_at_percent_rank", F.min("days_since_created_at_percent_rank").over(w4))
    .withColumn("element_avg_days_since_created_at_dense_rank_2", F.avg("days_since_created_at_dense_rank_2").over(w4))
    .withColumn("days_since_created_at_count_15m", F.count("days_since_created_at").over(w5))
)

result = (
    scored
    .groupBy(
        F.when(F.col("days_since_created_at_dense_rank_2") >= 8, "LARGE").otherwise("SMALL").alias("days_since_created_at_dense_rank_2_tier"),
        "element",
        F.date_trunc("hour", "created_at").alias("hour_window"),
    )
    .agg(
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
        F.min("days_since_created_at_dense_rank_2").alias("min_days_since_created_at_dense_rank_2"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_count"),
        F.count(F.when(F.col("element") == "WELCOME5", 1)).alias("welcome5_rows"),
    )
    .withColumn("spring10_count_percent_rank", F.percent_rank().over(w6))
)
