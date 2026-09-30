from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("months_since_last_restock").desc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([3, 8, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
        (F.datediff(F.current_date(), "last_restock") / 30).alias("months_since_last_restock"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "product_id")
)

with_dims = (
    latest
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.region"))
)

valid = (
    with_dims
    .filter((~F.col("region").eqNullSafe(F.lit("East"))) & F.col("store_id").isin([2, 3, 5]))
    .select(
        "*",
        F.when(F.col("region") == "Central", 1).otherwise(0).alias("is_central"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

result = (
    valid
    .select("id", "product_id", "region_clean")
    .orderBy(F.col("product_id").desc_nulls_first())
)
