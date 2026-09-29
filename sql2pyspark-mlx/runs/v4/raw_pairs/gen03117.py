from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
    )
)

cleaned = (
    lines
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.col("created_at").cast("date").alias("created_at_day_2"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.age"))
)

filtered = (
    enriched
    .filter(F.col("product_id") == 6)
    .withColumn("qty_rounded", F.round("qty", 0))
    .withColumn("age_band", F.when(F.col("age") >= 57, "High").when(F.col("age") >= 30, "Medium").otherwise("Low"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"), F.col("p.name"))
)

metrics = (
    with_dims
    .withColumn("customer_id_min_product_id", F.min("product_id").over(w))
    .withColumn("product_id_sum_30m", F.sum("product_id").over(w2))
)

result = (
    metrics
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "created_at_day",
        "age",
        "qty_rounded",
        "email",
        "name",
        "customer_id_min_product_id",
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("age").desc_nulls_last(), F.col("qty_rounded").desc())
)
