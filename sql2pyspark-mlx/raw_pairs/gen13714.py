from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy(F.month("last_restock")).orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("row_seq").desc_nulls_last(), "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("event_seq").desc(), "id")
w6 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "product_id", "last_restock")
)

metrics = (
    deduped
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w).alias("row_seq_2"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "product_id",
        "last_restock",
        "row_seq",
        "event_seq",
        "row_seq_2",
        "event_seq_2",
        "rank_in_group",
        F.row_number().over(w5).alias("seq_no_2"),
        F.first("seq_no", ignorenulls=True).over(w6).alias("first_known_seq_no"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.event_seq",
        "src.event_seq_2",
        "src.rank_in_group",
        "src.seq_no_2",
        "src.first_known_seq_no",
        F.col("b.brand"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("first_known_seq_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("first_known_seq_no_tier"),
        F.when(F.col("seq_no_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("seq_no_2_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("first_known_seq_no", [0.25, 0.5, 0.75]).alias("first_known_seq_no_quartiles"),
        F.percentile_approx("event_seq_2", [0.25, 0.5, 0.75]).alias("event_seq_2_quartiles"),
    )
    .orderBy("seq_no_2_tier")
)
