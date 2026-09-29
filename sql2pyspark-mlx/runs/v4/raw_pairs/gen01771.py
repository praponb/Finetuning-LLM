from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("element", "customer_id").orderBy("days_since_created_at_day")
w2 = Window.orderBy(F.col("days_since_created_at_day").asc_nulls_last())
w3 = Window.partitionBy("element").orderBy("days_since_created_at_day")
w4 = Window.partitionBy("element")
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

valid = (
    exploded
    .filter(F.col("element").isNotNull() & F.col("element").isin(["SPRING10", "VIP"]))
    .select("*", F.col("created_at").cast("date").alias("created_at_day"))
)

filtered = (
    valid
    .filter(F.col("created_at") >= F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "element",
        "created_at_day",
        F.initcap(F.trim("element")).alias("element_title"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.signup_date"))
)

filtered_carts = (
    enriched
    .filter(F.col("created_at") < "2023-05-09")
    .select(
        "id",
        "customer_id",
        "created_at",
        "element",
        "created_at_day",
        "element_title",
        "signup_date",
        F.datediff(F.current_date(), "created_at_day").alias("days_since_created_at_day"),
    )
)

metrics = (
    filtered_carts
    .withColumn("days_since_created_at_day_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("days_since_created_at_day_dense_rank_2", F.dense_rank().over(w2))
)

metrics_carts = (
    metrics
    .select(
        "*",
        F.rank().over(w3).alias("days_since_created_at_day_rank"),
        F.min("days_since_created_at_day_dense_rank_2").over(w4).alias("element_min_days_since_created_at_day_dense_rank_2"),
        F.count("days_since_created_at_day").over(w5).alias("days_since_created_at_day_count_15m"),
    )
)

result = (
    metrics_carts
    .groupBy(
        F.when(F.col("days_since_created_at_day_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("days_since_created_at_day_dense_rank_tier"),
        F.when(F.col("days_since_created_at_day_count_15m") >= 18, "HIGH").otherwise("LOW").alias("days_since_created_at_day_count_15m_tier"),
    )
    .agg(
        F.count_if(F.col("element") == "SPRING10").alias("spring10_events"),
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_rows_2"),
    )
)
