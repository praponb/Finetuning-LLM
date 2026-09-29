from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("value").orderBy(F.col("qty").desc_nulls_last())
w2 = Window.orderBy(F.col("qty").desc())
w3 = Window.partitionBy("customer_id").orderBy("qty")
w4 = Window.orderBy("qty")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

flattened_carts = (
    flattened
    .select("customer_id", "pos", "qty", F.explode_outer("coupon_codes").alias("value"))
)

with_windows = flattened_carts.withColumn("qty_cume_dist", F.cume_dist().over(w_hist))

metrics = (
    with_windows
    .filter(F.col("pos") != 3)
    .select(
        "customer_id",
        "pos",
        "qty",
        F.dense_rank().over(w2).alias("qty_dense_rank"),
    )
)

scored = (
    metrics
    .select(
        "customer_id",
        "pos",
        F.cume_dist().over(w3).alias("qty_cume_dist"),
        F.rank().over(w4).alias("qty_rank"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.customer_id",
        "src.pos",
        "src.qty_cume_dist",
        "src.qty_rank",
        F.col("d.name"),
        F.col("d.city"),
    )
)

result = (
    with_dims
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "qty_rank", "city", F.col("max_order_discount"))
    .orderBy(F.col("city").desc())
)
