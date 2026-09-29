from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc_nulls_last(), "id")
w_grp = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

ranked = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category") == "Books")
    .select("a.*", F.col("p.stock_qty"))
)

metrics = (
    enriched
    .select(
        "id",
        "product_id",
        "sale_date",
        "rn",
        "stock_qty",
        F.lead("units").over(w_grp).alias("next_units"),
        F.lead("revenue").over(w_grp).alias("next_revenue"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.rn",
        "s.next_units",
        "s.event_seq",
        F.col("d.category"),
        F.col("d.brand"),
    )
)

enriched_store_sales = (
    joined
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.price"))
)

pivoted = (
    enriched_store_sales
    .groupBy("product_id")
    .pivot("brand", ["Stark", "Umbrella"])
    .agg(F.sum("price").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "product_id",
        F.col("Stark_total").alias("stark_total"),
        F.col("Stark_item_count").alias("stark_item_count"),
        F.col("Umbrella_total").alias("umbrella_total"),
        F.col("Umbrella_item_count").alias("umbrella_item_count"),
    )
)

result = (
    pivoted
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select("product_id", F.col("max_order_item_unit_price"))
    .orderBy(F.col("product_id").desc_nulls_first())
)
