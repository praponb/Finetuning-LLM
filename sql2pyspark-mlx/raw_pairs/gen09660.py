from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "last_restock"), "store_id").orderBy("quantity", "id")
w3 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy(F.col("last_restock").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

metrics = (
    df_inventory
    .filter(F.col("quantity") >= 68)
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w_grp))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

enriched = (
    metrics
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.store_name"), F.col("d.region"), F.col("d.sqft"))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

base = (
    deduped
    .filter(F.col("region").isNotNull() & F.col("store_id").isin([1, 2]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "first_known_quantity",
        "prev_quantity",
        "event_seq",
        "store_name",
        "region",
        "latest_rank",
        F.ceil("event_seq").alias("event_seq_whole"),
        F.round((F.col("sqft") * 0.9), 2).alias("sqft_adjusted"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "prev_quantity",
        "event_seq",
        "store_name",
        "region",
        "latest_rank",
        "event_seq_whole",
        "sqft_adjusted",
    )
)

result = (
    ranked
    .select(
        "id",
        "prev_quantity",
        "store_name",
        "latest_rank",
        "event_seq_whole",
        "sqft_adjusted",
    )
    .orderBy(F.col("event_seq_whole").asc_nulls_last())
)
