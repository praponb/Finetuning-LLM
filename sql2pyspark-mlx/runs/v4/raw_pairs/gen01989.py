from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_first())
w4 = Window.partitionBy("brand").orderBy(F.col("units").asc_nulls_first(), "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

ranked = (
    df_store_sales
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims = (
    latest
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.region"))
)

latest_store_sales = (
    with_dims
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest_store_sales
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.stock_qty"), F.col("b.brand"))
)

ranked_store_sales = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked_store_sales
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "region",
        "brand",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("region").desc_nulls_first())
)
