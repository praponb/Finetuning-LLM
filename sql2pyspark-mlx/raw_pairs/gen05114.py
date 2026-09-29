from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("line_total").desc_nulls_first())
w2 = Window.orderBy(F.col("unit_price").desc(), "id")
w3 = Window.orderBy(F.col("unit_price").asc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id") == 5)
    .select(
        "*",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total_2"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "unit_price", "line_total")
)

base = (
    deduped
    .filter(F.col("unit_price") >= 250)
    .select("*", F.floor("line_total").alias("line_total_whole"))
)

enriched = (
    base
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w3))
)

filtered = (
    enriched
    .filter(F.col("line_total") <= 23)
    .select(
        "id",
        "unit_price",
        "line_total",
        "line_total_whole",
        "seq_no",
        "unit_price_dense_rank",
        F.ceil("line_total_whole").alias("line_total_whole_whole"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") >= 350)
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("quantity").between(5, 6))
        .select("id")
    )
)

verified_order_items = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified_order_items
    .select(
        "line_total",
        "line_total_whole",
        "seq_no",
        "unit_price_dense_rank",
        "line_total_whole_whole",
    )
    .orderBy(F.col("line_total_whole_whole").desc())
)
