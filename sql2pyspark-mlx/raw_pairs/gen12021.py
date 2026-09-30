from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", "coupon_codes", F.col("line.product_id"))
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "a.customer_id",
        "a.product_id",
        F.col("d.email"),
        F.col("d.signup_date"),
        F.col("d.segment"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.credit_limit"))
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.customer_id",
        "a.product_id",
        "a.email",
        "a.segment",
        F.col("p.age"),
        F.col("p.name"),
    )
)

filtered = (
    joined
    .filter(F.col("age") <= 41)
    .select(
        "customer_id",
        "product_id",
        "email",
        "segment",
        "age",
        "name",
        F.trim("email").alias("clean_email"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("age_share") >= 0.78, "HIGH").otherwise("LOW").alias("age_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.count("*").alias("num_rows"),
    )
)

result = grouped.select("age_share_tier", "consumer_rows", "num_rows")
