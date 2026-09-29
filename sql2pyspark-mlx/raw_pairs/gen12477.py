from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("running_sum_running_count_quantity", "id")
w6 = Window.orderBy("running_avg_quantity", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.orderBy(F.col("running_avg_quantity").desc(), "id")
w8 = Window.partitionBy("store_id").orderBy("quantity")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-03-04"))) & (F.year("last_restock") == 2023))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.opened_date"), F.col("s.store_name"))
)

scored = (
    enriched
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.avg("quantity").over(w).alias("running_avg_quantity"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "running_count_quantity",
        "running_avg_quantity",
        F.max("running_count_quantity").over(w_grp).alias("running_max_running_count_quantity"),
        F.sum("running_count_quantity").over(w_grp).alias("running_sum_running_count_quantity"),
        F.avg("running_count_quantity").over(w_grp).alias("running_avg_running_count_quantity"),
        F.sum("running_max_quantity").over(w3).alias("store_id_sum_running_max_quantity"),
    )
)

metrics = (
    with_windows
    .withColumn("quantity_avg_5", F.avg("quantity").over(w4))
    .withColumn("next_store_id_sum_running_max_quantity", F.lead("store_id_sum_running_max_quantity", 2).over(w5))
    .withColumn("running_max_running_count_quantity_avg_5", F.avg("running_max_running_count_quantity").over(w6))
    .withColumn("running_avg_quantity_bucket", F.ntile(5).over(w7))
    .withColumn("quantity_rank", F.rank().over(w8))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("quantity") >= 191, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("next_store_id_sum_running_max_quantity", [0.25, 0.5, 0.75]).alias("next_store_id_sum_running_max_quantity_quartiles"),
        F.percentile_approx("quantity_rank", [0.25, 0.5, 0.75]).alias("quantity_rank_quartiles"),
        F.count("*").alias("num_rows"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("running_sum_running_count_quantity"), 2).alias("avg_running_sum_running_count_quantity"),
        F.max("next_store_id_sum_running_max_quantity").alias("max_next_store_id_sum_running_max_quantity"),
    )
)
