from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

metrics = (
    df_inventory
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.ntile(3).over(w2).alias("quantity_bucket"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.last_restock",
        "s.quantity_bucket",
        "s.rank_in_group",
        F.col("d.category"),
        F.col("d.brand"),
        F.col("d.product_name"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.product_name").alias("product_product_name"))
)

result = (
    with_dims
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "rank_in_group",
        "category",
        "brand",
        "product_product_name",
        F.when(F.col("product_product_name").rlike("^(Noise-Cancel Headphones|wireless mouse)$"), "PRIORITY").otherwise("EXTENDED").alias("product_product_name_zone"),
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("product_product_name_zone").desc_nulls_last())
)
