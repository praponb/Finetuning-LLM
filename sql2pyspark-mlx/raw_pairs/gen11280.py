from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("price", "id")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-04-06").cast("date")) & (F.col("last_restock") <= "2025-07-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

valid = (
    filtered
    .filter(F.col("store_id").isin([1, 3, 4]))
    .withColumn("quantity_whole_whole", F.floor("quantity_whole"))
    .withColumn("years_since_last_restock", (F.datediff(F.current_date(), "last_restock") / 365.25))
)

with_dims = (
    valid
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .filter(F.year(F.col("d.opened_date")) >= 2025)
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity_whole_whole",
        F.col("d.city"),
        F.col("d.region"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.region",
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.price"),
    )
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "category")
)

result = (
    ranked
    .join(revenue_by_store_id, on="store_id", how="left")
    .select("id", "store_id", F.col("max_store_sale_revenue"))
)
