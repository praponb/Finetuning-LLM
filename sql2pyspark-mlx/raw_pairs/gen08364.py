from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("duration_min").desc())

lines = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

with_dims = (
    lines
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.segment"))
)

cleaned = (
    with_dims
    .filter(F.col("segment").isNotNull() & (F.col("promo").eqNullSafe(F.lit("VIP"))))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
    .withColumn("promo_upper", F.upper("promo"))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("OTHER")))
)

with_dims_carts = (
    cleaned
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.country") == "UK")
    .select("a.*", F.col("p.city"), F.col("p.credit_limit"), F.col("p.name"))
)

result = (
    with_dims_carts
    .groupBy("segment", "product_id")
    .agg(
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("days_since_created_at")).otherwise(0)).alias("welcome5_days_since_created_at"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("price_per_singapore"),
        F.min("created_at").alias("first_created_at"),
    )
    .withColumn("duration_min_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("segment").desc_nulls_last())
)
