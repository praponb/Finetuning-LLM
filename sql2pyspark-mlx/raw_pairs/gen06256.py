from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").asc_nulls_last())

metrics = (
    df_inventory
    .withColumn("prev_quantity", F.lag("quantity").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

cleaned = (
    metrics
    .filter(F.col("product_id").isin([1, 4, 6, 10]))
    .withColumn("seq_no_per_quantity", (F.col("seq_no") / F.nullif("quantity", F.lit(0))))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.store_name"), F.col("d.opened_date"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.sqft"))
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "prev_quantity",
        "seq_no",
        "seq_no_per_quantity",
        "store_name",
        "opened_date",
    )
)
