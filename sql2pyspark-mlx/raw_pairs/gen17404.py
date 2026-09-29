from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 4, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round("quantity", 0).alias("quantity_rounded"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "quantity")
)

valid = (
    deduped
    .filter(F.col("quantity") >= 134)
    .select("*", F.ceil("quantity").alias("quantity_whole"))
)

filtered = (
    valid
    .filter(F.col("quantity_whole") == 41)
    .withColumn("quantity_minus_quantity_whole", (F.col("quantity") - F.col("quantity_whole")))
    .withColumn("quantity_whole_filled", F.coalesce("quantity_whole", F.lit(0)))
)

result = (
    filtered
    .alias("e")
    .join(df_stores.alias("dim"), F.col("e.store_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.quantity_whole_filled")).alias("min_quantity_whole_filled"),
        F.grouping_id().alias("level_id"),
    )
)
