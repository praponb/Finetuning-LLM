from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2024-01-19")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.brand"), F.col("p.product_name"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.category"), F.col("d.price"), F.col("d.launch_date"))
)

result = (
    with_dims
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "brand",
        "product_name",
        "latest_rank",
        "category",
        "price",
        F.when(F.col("brand").rlike("^(Acme|Initech)$"), "CORE").when(F.col("brand").rlike("^(Globex|Umbrella)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("brand_zone"),
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
