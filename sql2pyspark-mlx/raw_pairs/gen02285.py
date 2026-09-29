from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("region", "city").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("order_shipping_fee").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & ~F.col("city").isin(["Singapore", "Tokyo"]))
    .withColumn("opened_date_quarter", F.quarter("opened_date"))
    .withColumn("sqft_rounded", F.round("sqft", 0))
)

latest_match = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        F.col("c.amount"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

metrics = (
    latest_match
    .filter(F.col("store_name").like("Air%"))
    .select(
        "id",
        "region",
        "amount",
        F.ntile(10).over(w2).alias("amount_bucket"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.dense_rank().over(w4).alias("order_shipping_fee_dense_rank"),
    )
)

latest_match_stores = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

eligible = (
    latest_match_stores
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.last_restock")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "region",
        "amount",
        "amount_bucket",
        "first_known_amount",
        "last_restock",
        "quantity",
    )
    .orderBy("first_known_amount")
)
