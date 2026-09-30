from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("category", "supplier_id")
w3 = Window.partitionBy("brand", "supplier_id").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("cost_avg_6_2_per_corr_price_stock_qty", "id")
w5 = Window.orderBy(F.col("cost_avg_6_2").desc(), "id")

with_windows = (
    df_products
    .select(
        "id",
        "cost",
        F.avg("cost").over(w).alias("cost_avg_6"),
        F.corr("price", "stock_qty").over(w2).alias("corr_price_stock_qty"),
        F.avg("cost").over(w3).alias("cost_avg_6_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("corr_price_stock_qty") <= -0.4)
    .withColumn("cost_avg_6_2_per_corr_price_stock_qty", (F.col("cost_avg_6_2") / F.nullif("corr_price_stock_qty", F.lit(0))))
)

filtered = (
    valid
    .filter(F.col("cost_avg_6_2_per_corr_price_stock_qty").between(22, 68))
    .withColumn("cost_band", F.when(F.col("cost") >= 650, "High").when(F.col("cost") >= 270, "Medium").otherwise("Low"))
)

with_windows_products = (
    filtered
    .withColumn("prev_cost_avg_6_2", F.lag("cost_avg_6_2").over(w4))
    .withColumn("prev_cost_avg_6", F.lag("cost_avg_6").over(w4))
    .withColumn("next_cost_avg_6", F.lead("cost_avg_6").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("event_seq_2", F.row_number().over(w5))
)

base = (
    with_windows_products
    .filter(F.col("next_cost_avg_6") < 286)
    .select(
        "id",
        "cost_avg_6",
        "corr_price_stock_qty",
        "cost_band",
        "prev_cost_avg_6_2",
        "prev_cost_avg_6",
        "next_cost_avg_6",
        "event_seq",
        "event_seq_2",
        F.col("event_seq_2").cast("string").alias("event_seq_2_str"),
        (F.col("corr_price_stock_qty") * F.col("prev_cost_avg_6_2")).alias("corr_price_stock_qty_times_prev_cost_avg_6_2"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("prev_cost_avg_6_2") >= 581, "HIGH").otherwise("LOW").alias("prev_cost_avg_6_2_tier"),
        F.when(F.col("prev_cost_avg_6") >= 711, "LARGE").otherwise("SMALL").alias("prev_cost_avg_6_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("corr_price_stock_qty_times_prev_cost_avg_6_2"), 2).alias("avg_corr_price_stock_qty_times_prev_cost_avg_6_2"),
        F.percentile_approx("cost_avg_6", 0.9).alias("cost_avg_6_p90"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
