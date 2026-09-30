from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

flattened = (
    exploded
    .select(
        "customer_id",
        "created_at",
        "key",
        F.explode("coupon_codes").alias("element"),
    )
)

cleaned = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("element").isNotNull())
    .select(
        "*",
        F.date_trunc("MONTH", "created_at").alias("created_at_period"),
        F.date_trunc("week", "created_at").alias("created_at_period_2"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"), F.col("d.name"), F.col("d.segment"))
)

scored = (
    joined
    .select(
        "element",
        "credit_limit",
        "name",
        "segment",
        F.max("credit_limit").over(w).alias("credit_limit_max_2h"),
    )
)

filtered = (
    scored
    .filter(F.col("segment").isNotNull())
    .withColumn("credit_limit_rounded", F.round("credit_limit", 2))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("UNKNOWN")))
)

filtered_carts = (
    filtered
    .filter(F.col("element").isin(["VIP", "WELCOME5", "BUNDLE"]))
    .select(
        "element",
        "name",
        "segment",
        "credit_limit_max_2h",
        "credit_limit_rounded",
        "segment_clean",
        F.upper("segment").alias("segment_upper"),
    )
)

result = (
    filtered_carts
    .rollup(
        F.when(F.col("credit_limit_rounded") >= 62, "HIGH").otherwise("LOW").alias("credit_limit_rounded_tier"),
        "element",
    )
    .agg(
        F.round(F.avg("credit_limit_rounded"), 2).alias("avg_credit_limit_rounded"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
    )
)
