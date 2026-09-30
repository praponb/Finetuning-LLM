from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock")

enriched = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand") == "Initech")
    .select("a.*", F.col("p.product_name"), F.col("p.launch_date"), F.col("p.category"))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    deduped
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.price"),
        F.col("p.launch_date").alias("product_launch_date"),
        F.col("p.brand"),
    )
)

latest = (
    with_dims
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_id",
        "product_name",
        "category",
        "latest_rank",
        "price",
        "brand",
    )
)

result = (
    latest
    .withColumn("price_tier", F.when(F.col("price") >= 1080, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["price_tier", "category"], ["price_tier"], []],
        "price_tier",
        "category",
    )
    .agg(
        F.max("price").alias("max_price"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("price"), 2).alias("avg_price_2"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("avg_price_2") >= 0)
    .orderBy("price_tier")
)
