from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_first())
w3 = Window.orderBy("launch_date", "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-01-18").cast("date"))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

enriched = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.price"), F.col("p.product_name"), F.col("p.launch_date"))
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

joined = (
    enriched_inventory
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.region"), F.col("p2.sqft"), F.col("p2.opened_date"))
)

result = (
    joined
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "product_name",
        "quantity_cume_dist",
        "sqft",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("quantity_cume_dist").desc_nulls_last())
)
