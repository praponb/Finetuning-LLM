from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_first())
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([8, 16, 37, 40]))
    .withColumn("least_quantity_unit_price", F.least("quantity", "unit_price"))
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

remaining_keys = (
    filtered
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") > 4)
        .select("product_id", "order_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_cume_dist", F.cume_dist().over(w))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w2))
)

with_dims = (
    metrics
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.status"), F.col("d.amount"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select(
        "e.order_id",
        "e.product_id",
        "e.quantity",
        "e.unit_price",
        "e.least_quantity_unit_price",
        "e.quantity_cume_dist",
        "e.quantity_dense_rank",
        "e.status",
        F.col("p.shipping_fee"),
    )
)

result = (
    joined
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "unit_price",
        "quantity_cume_dist",
        "status",
        "shipping_fee",
        F.col("max_payment_amount"),
    )
    .orderBy(F.col("max_payment_amount").desc())
)
