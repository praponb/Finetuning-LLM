from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id").orderBy("running_max_quantity", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("product_id")
w_ordered = Window.partitionBy("store_id").orderBy("running_sum_quantity", "id")
w5 = Window.partitionBy("brand", "product_id").orderBy(F.col("prev_running_avg_quantity").desc_nulls_first(), "id")
w6 = Window.partitionBy(F.year("last_restock"), "brand").orderBy("next_running_avg_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("brand").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("row_seq", "id").rowsBetween(-3, -1)

with_windows = (
    df_inventory
    .withColumn("running_max_quantity", F.max("quantity").over(w))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w))
    .withColumn("running_sum_quantity", F.sum("quantity").over(w))
)

scored = (
    with_windows
    .withColumn("last3_min_quantity", F.min("quantity").over(w_part))
    .withColumn("last3_avg_quantity", F.avg("quantity").over(w_part))
    .withColumn("last3_max_quantity", F.max("quantity").over(w_part))
    .withColumn("product_id_max_running_sum_quantity", F.max("running_sum_quantity").over(w_grp))
)

enriched = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.brand"))
)

metrics = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "running_avg_quantity",
        "last3_avg_quantity",
        "last3_max_quantity",
        "brand",
        F.lead("running_avg_quantity").over(w_ordered).alias("next_running_avg_quantity"),
        F.lag("running_avg_quantity").over(w_ordered).alias("prev_running_avg_quantity"),
        F.lead("quantity").over(w_ordered).alias("next_quantity"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_inventory = (
    latest
    .select(
        "*",
        F.sum("prev_running_avg_quantity").over(w6).alias("cumulative_prev_running_avg_quantity"),
        F.first("last3_max_quantity", ignorenulls=True).over(w7).alias("first_known_last3_max_quantity"),
        F.avg("next_running_avg_quantity").over(w8).alias("next_running_avg_quantity_avg_3_prior"),
    )
)

result = (
    enriched_inventory
    .cube(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        F.when(F.col("cumulative_prev_running_avg_quantity") >= 926, "PREMIUM").otherwise("STANDARD").alias("cumulative_prev_running_avg_quantity_tier"),
    )
    .agg(
        F.avg("first_known_last3_max_quantity").alias("avg_first_known_last3_max_quantity"),
        F.round(F.avg("last3_max_quantity"), 2).alias("avg_last3_max_quantity"),
    )
    .orderBy(F.col("avg_first_known_last3_max_quantity").desc_nulls_last())
)
