from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w_running = Window.partitionBy("product_id").orderBy("row_seq", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("product_id", "store_id").orderBy("row_seq", "id")
amount_by_store_id = df_orders.groupBy("store_id").agg(F.min("amount").alias("min_order_amount"))

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-03-31"))) & F.col("quantity").between(98, 127))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 260, "High").when(F.col("quantity") >= 100, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

enriched = (
    valid
    .withColumn("quantity_bucket", F.ntile(3).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.min("quantity_bucket").over(w_running).alias("last4_min_quantity_bucket"),
        F.max("quantity_bucket").over(w_running).alias("last4_max_quantity_bucket"),
        F.count("quantity_bucket").over(w_running).alias("last4_count_quantity_bucket"),
        F.sum("quantity_bucket").over(w_running).alias("last4_sum_quantity_bucket"),
        F.ntile(3).over(w5).alias("row_seq_bucket"),
    )
)

valid_inventory = (
    metrics
    .filter(F.col("row_seq_bucket").between(5, 7))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity_level",
        "quantity_bucket",
        "row_seq",
        "seq_no",
        "last4_count_quantity_bucket",
        "last4_sum_quantity_bucket",
        "row_seq_bucket",
        F.ceil("row_seq_bucket").alias("row_seq_bucket_whole"),
    )
)

remaining_keys = (
    valid_inventory
    .select("store_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") < 68)
        .select("store_id")
    )
)

reconciled = (
    valid_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity_level",
        "quantity_bucket",
        "row_seq",
        "seq_no",
        "last4_sum_quantity_bucket",
        "row_seq_bucket",
        "row_seq_bucket_whole",
        F.col("min_order_amount"),
    )
)
