from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("unit_price_whole").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("unit_price_whole", "id")
w5 = Window.partitionBy("order_id").orderBy("unit_price_filled", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("order_id").orderBy(F.col("row_seq_str").desc())
w8 = Window.orderBy(F.col("row_seq").asc_nulls_first())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.floor("unit_price").alias("unit_price_whole"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_whole",
        F.sum("unit_price_whole").over(w).alias("running_unit_price_whole"),
        F.last("unit_price_whole", ignorenulls=True).over(w2).alias("last_known_unit_price_whole"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(10).over(w4).alias("unit_price_whole_bucket"),
        F.avg("unit_price_filled").over(w5).alias("unit_price_filled_avg_2_prior"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(120, 220))
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_order_items = (
    kept_rows
    .filter(F.col("quantity").between(3, 6))
    .select(
        "order_id",
        "quantity",
        "last_known_unit_price_whole",
        "row_seq",
        F.col("row_seq").cast("double").alias("row_seq_str"),
    )
)

result = (
    cleaned_order_items
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w6)), 4))
    .withColumn("order_id_sum_quantity", F.sum("quantity").over(w6))
    .withColumn("row_seq_str_dense_rank", F.dense_rank().over(w7))
    .withColumn("row_seq_rank", F.rank().over(w8))
)
