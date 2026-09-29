from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("last_restock").desc_nulls_first())
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-06-12").cast("date"))
    .select("*", F.date_sub("last_restock", 58).alias("last_restock_minus_58d"))
)

enriched = (
    valid
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.brand"), F.col("p.category"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.store_name"), F.col("b.region"), F.col("b.sqft"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "last_restock_minus_58d",
        "brand",
        "region",
        "sqft",
        "rank_in_group",
        F.when(F.col("region").rlike("^(West|North)$"), "PRIORITY").otherwise("EXTENDED").alias("region_zone"),
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("max_order_shipping_fee").desc_nulls_first())
)
