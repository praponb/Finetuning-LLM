from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount_per_discount", "id")
w2 = Window.orderBy(F.col("shipping_fee").asc_nulls_first())
w3 = Window.partitionBy("store_id", "customer_id").orderBy("order_date", "id")

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
        F.concat("channel", F.lit(", "), "status").alias("channel_status"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

metrics = (
    filtered
    .withColumn("amount_per_discount_bucket", F.ntile(5).over(w))
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w2))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.age"), F.col("c.email"), F.col("c.country"))
)

matched_keys = (
    with_dims
    .select("id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("status") == "SHIPPED")
        .select("id", "customer_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.store_name"), F.col("b.region"), F.col("b.opened_date"))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "channel_status",
        "channel_clean",
        "age",
        "email",
    )
    .orderBy("order_date", "customer_id")
)
