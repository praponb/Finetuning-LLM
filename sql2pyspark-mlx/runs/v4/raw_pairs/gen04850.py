from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("quantity", "id")
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-07-24")))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 0.85), 2))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"), F.col("p.stock_qty"))
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "adjusted_quantity",
        "stock_qty",
        "dedup_rank_2",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
