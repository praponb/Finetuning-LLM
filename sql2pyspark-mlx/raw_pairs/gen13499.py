from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w_part = Window.partitionBy(F.month("last_restock"), "product_id")
w3 = Window.orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

with_windows = (
    df_inventory
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_restock_month_stddev_samp_quantity", F.stddev_samp("quantity").over(w_part))
    .withColumn("quantity_avg_2", F.avg("quantity").over(w3))
)

base = (
    with_windows
    .filter((F.col("last_restock") > F.lit("2023-09-14").cast("date")) & F.col("last_restock").between("2024-05-25", "2024-06-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "seq_no",
        "last_restock_month_stddev_samp_quantity",
        "quantity_avg_2",
        F.ceil("last_restock_month_stddev_samp_quantity").alias("last_restock_month_stddev_samp_quantity_whole"),
    )
)

filtered = (
    base
    .filter(F.col("quantity_avg_2") != 193)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock_month_stddev_samp_quantity_whole",
        F.round("seq_no", 1).alias("seq_no_rounded"),
    )
)

matched_keys = (
    filtered
    .select("id", "product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock") >= "2025-09-02")
        .select("id", "product_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

summary = (
    verified
    .groupBy("product_id")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("seq_no_rounded"), 2).alias("avg_seq_no_rounded"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    summary
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "event_count",
        "distinct_ids",
        "avg_seq_no_rounded",
        "avg_quantity",
        F.col("avg_store_sale_revenue"),
    )
)
