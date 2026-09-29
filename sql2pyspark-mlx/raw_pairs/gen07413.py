from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("quantity").desc())

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-24").cast("date")) & (F.col("quantity") <= 174))
    .withColumn("last_restock_label", F.date_format("last_restock", "MMM yyyy"))
)

enriched = (
    base
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.brand"), F.col("b.stock_qty"), F.col("b.launch_date"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.year(F.col("d.launch_date")) >= 2025)
    .select(
        "a.*",
        F.col("d.price"),
        F.col("d.stock_qty").alias("product_stock_qty"),
        F.col("d.product_name"),
    )
)

latest = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims_inventory = (
    latest
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.brand",
        "e.stock_qty",
        "e.price",
        "e.product_stock_qty",
        "e.latest_rank",
        F.col("p.category"),
    )
)

result = with_dims_inventory.select("id", "price", "product_stock_qty")
