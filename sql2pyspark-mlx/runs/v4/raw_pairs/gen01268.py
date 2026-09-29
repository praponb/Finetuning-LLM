from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_first())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_order_items
    .filter(F.col("unit_price") > 260)
    .select(
        "*",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total_2"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.discount"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total_2",
        F.col("max_inventory_quantity"),
    )
)
