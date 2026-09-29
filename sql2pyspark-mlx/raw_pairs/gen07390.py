from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "product_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.year("opened_date"), "product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.orderBy(F.col("last_restock").desc(), "id")
w5 = Window.partitionBy("region").orderBy("opened_date", "id")
w6 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("city").orderBy(F.col("seq_no").desc(), "id")
w8 = Window.partitionBy("store_id", "city")
w9 = Window.partitionBy("store_id", "city").orderBy("seq_no_num", "id").rowsBetween(-3, Window.currentRow)

with_dims = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.city").isin(["Berlin", "Singapore"]))
    .select("s.*", F.col("p2.store_name"), F.col("p2.opened_date"), F.col("p2.region"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "store_id",
        "last_restock",
        "store_name",
        "opened_date",
        F.avg("quantity").over(w).alias("quantity_avg_2_prior"),
        F.lag("quantity", 2).over(w2).alias("prev_quantity"),
        F.sum("quantity").over(w3).alias("product_id_sum_quantity"),
        F.row_number().over(w4).alias("seq_no"),
        F.lead("quantity", 1).over(w5).alias("next_quantity"),
    )
)

valid = (
    with_windows
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-07-10")))
    .withColumn("seq_no_num", F.col("seq_no").cast("double"))
)

with_dims_inventory = (
    valid
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.last_restock",
        "s.store_name",
        "s.opened_date",
        "s.prev_quantity",
        "s.product_id_sum_quantity",
        "s.seq_no",
        "s.next_quantity",
        "s.seq_no_num",
        F.col("d.store_name").alias("store_store_name"),
        F.col("d.city"),
    )
)

with_windows_inventory = (
    with_dims_inventory
    .withColumn("prev_quantity_avg_2_prior", F.avg("prev_quantity").over(w6))
    .withColumn("seq_no_bucket", F.ntile(10).over(w7))
    .withColumn("store_id_max_product_id_sum_quantity", F.max("product_id_sum_quantity").over(w8))
    .withColumn("next_quantity_avg_3", F.avg("next_quantity").over(w9))
)

stats = (
    with_windows_inventory
    .groupBy(
        F.when(F.col("next_quantity_avg_3") >= 211, "PREMIUM").otherwise("STANDARD").alias("next_quantity_avg_3_tier"),
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("prev_quantity_avg_2_prior", 0.9).alias("prev_quantity_avg_2_prior_p90"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.max(F.when(F.col("city") == "London", F.col("product_id_sum_quantity"))).alias("london_max_product_id_sum_quantity"),
    )
)

result = (
    stats
    .select(
        "next_quantity_avg_3_tier",
        "seq_no_tier",
        "bangkok_count",
        "london_max_product_id_sum_quantity",
    )
    .orderBy("next_quantity_avg_3_tier")
)
