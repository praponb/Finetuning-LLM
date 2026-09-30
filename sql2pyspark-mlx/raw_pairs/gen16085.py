from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("adjusted_quantity", "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("order_id").orderBy(F.col("adjusted_quantity_bucket").desc_nulls_first(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_first(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 180))
    .select(
        "*",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
)

enriched = (
    cleaned
    .filter(F.col("order_id") == 25)
    .select(
        "id",
        "order_id",
        "quantity",
        "adjusted_quantity",
        F.ntile(3).over(w).alias("adjusted_quantity_bucket"),
        F.avg("quantity").over(w2).alias("quantity_avg_5_prior"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.shipping_fee"), F.col("d.amount"))
)

latest = (
    with_dims
    .withColumn("dedup_rank_2", F.row_number().over(w4))
    .filter(F.col("dedup_rank_2") == 1)
)

enriched_order_items = (
    latest
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.discount"))
)

kept_keys = (
    enriched_order_items
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price") >= 260)
        .select("id")
    )
)

kept_rows = (
    enriched_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "adjusted_quantity",
        "adjusted_quantity_bucket",
        "dedup_rank",
        "shipping_fee",
        "dedup_rank_2",
        F.col("sum_payment_amount"),
    )
)
