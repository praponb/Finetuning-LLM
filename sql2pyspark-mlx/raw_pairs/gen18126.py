from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity_rounded").desc(), "id")
w3 = Window.orderBy("quantity", "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id").orderBy("seq_no", "id").rowsBetween(-3, -1)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

with_windows = (
    cleaned
    .select(
        "id",
        "order_id",
        "quantity",
        "quantity_rounded",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("quantity_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

scored = (
    with_windows
    .filter(F.col("corr_quantity_unit_price") > 0.5)
    .select(
        "id",
        "quantity",
        "quantity_rounded",
        "event_seq",
        F.avg("corr_quantity_unit_price").over(w5).alias("order_id_avg_corr_quantity_unit_price"),
        F.avg("seq_no").over(w6).alias("seq_no_avg_3_prior"),
    )
)

cleaned_order_items = (
    scored
    .filter(F.col("event_seq") < 7)
    .withColumn("seq_no_avg_3_prior_rounded", F.round("seq_no_avg_3_prior", 0))
)

result = (
    cleaned_order_items
    .rollup(
        F.when(F.col("quantity_rounded") >= 90, "PREMIUM").otherwise("STANDARD").alias("quantity_rounded_tier"),
        F.when(F.col("order_id_avg_corr_quantity_unit_price") >= 1, "LARGE").otherwise("SMALL").alias("order_id_avg_corr_quantity_unit_price_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("seq_no_avg_3_prior", [0.25, 0.5, 0.75]).alias("seq_no_avg_3_prior_quartiles"),
        F.round(F.avg("order_id_avg_corr_quantity_unit_price"), 2).alias("avg_order_id_avg_corr_quantity_unit_price"),
        F.percentile_approx("seq_no_avg_3_prior", 0.95).alias("seq_no_avg_3_prior_p95"),
        F.percentile_approx("quantity_rounded", 0.95).alias("quantity_rounded_p95"),
    )
    .orderBy(F.col("avg_order_id_avg_corr_quantity_unit_price").desc_nulls_last())
)
