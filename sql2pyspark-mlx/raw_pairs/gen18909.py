from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("last_restock"), "store_id")
w5 = Window.orderBy("last_restock", "id")
w6 = Window.partitionBy("product_id").orderBy("running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id").orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)
w_recent = Window.orderBy("launch_date", "id").rowsBetween(-6, -1)
w9 = Window.orderBy("prev_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("last_restock_month_max_quantity_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("running_running_quantity").desc(), "id")
w12 = Window.orderBy(F.col("cumulative_event_seq").desc_nulls_last())
w13 = Window.partitionBy(F.month("last_restock")).orderBy("last_restock_month_max_quantity", "id").rowsBetween(-2, -1)
w14 = Window.partitionBy(F.month("launch_date")).orderBy("quantity", "id")
w15 = Window.orderBy("last_restock_month_max_quantity_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w_recent_2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .filter(F.col("quantity") == 238)
    .withColumn("quantity_rank", F.rank().over(w))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
    .withColumn("last_restock_month_max_quantity", F.max("quantity").over(w4))
)

enriched = (
    scored
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.brand"), F.col("p.launch_date"), F.col("p.stock_qty"))
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.lead("prev_quantity", 1).over(w5).alias("next_prev_quantity"),
        F.first("running_quantity", ignorenulls=True).over(w6).alias("first_known_running_quantity"),
        F.avg("quantity").over(w_running).alias("quantity_avg_3"),
        F.avg("last_restock_month_max_quantity").over(w_recent).alias("last_restock_month_max_quantity_avg_6_prior"),
        F.sum("running_quantity").over(w9).alias("running_running_quantity"),
    )
)

base = (
    enriched_inventory
    .filter(~F.col("brand").eqNullSafe(F.lit("Acme")))
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_rank",
        "event_seq",
        "running_quantity",
        "last_restock_month_max_quantity",
        "launch_date",
        "stock_qty",
        "next_prev_quantity",
        "first_known_running_quantity",
        "quantity_avg_3",
        "last_restock_month_max_quantity_avg_6_prior",
        "running_running_quantity",
        (F.col("event_seq") / F.nullif("next_prev_quantity", F.lit(0))).alias("event_seq_per_next_prev_quantity"),
    )
)

enriched_inventory_2 = (
    base
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w11))
)

metrics = (
    enriched_inventory_2
    .select(
        "*",
        F.dense_rank().over(w12).alias("cumulative_event_seq_dense_rank"),
        F.avg("running_running_quantity").over(w13).alias("running_running_quantity_avg_2_prior"),
        F.row_number().over(w14).alias("row_seq"),
        F.avg("quantity_avg_3").over(w15).alias("quantity_avg_3_avg_3"),
        F.sum("quantity_rank").over(w_recent_2).alias("running_quantity_rank"),
    )
)

result = (
    metrics
    .groupBy(
        "seq_no",
        F.when(F.col("last_restock_month_max_quantity") >= 198, "LARGE").otherwise("SMALL").alias("last_restock_month_max_quantity_tier"),
    )
    .agg(
        F.min("row_seq").alias("min_row_seq"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.min("seq_no").alias("min_seq_no"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("quantity_bit_or") >= 1)
    .orderBy(F.col("seq_no").desc_nulls_first())
)
