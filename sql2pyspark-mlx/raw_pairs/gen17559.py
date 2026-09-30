from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("supplier_id").orderBy(F.col("running_count_cost").desc(), "id")
w4 = Window.partitionBy("supplier_id")
w5 = Window.orderBy("stock_qty_scaled_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty_scaled_avg_3_prior").desc(), "id")
w7 = Window.partitionBy("supplier_id").orderBy(F.col("running_avg_cost").asc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") < "2024-09-30"))
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round((F.col("stock_qty") * 0.9), 2).alias("stock_qty_scaled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "stock_qty_scaled",
        F.count("cost").over(w).alias("running_count_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.avg("stock_qty_scaled").over(w2).alias("stock_qty_scaled_avg_3_prior"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.stddev_samp("stock_qty").over(w4).alias("supplier_id_stddev_samp_stock_qty"),
    )
)

enriched_products = (
    enriched
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.supplier_name"))
)

scored = (
    enriched_products
    .withColumn("running_running_sum_cost", F.sum("running_sum_cost").over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("running_avg_cost_percent_rank", F.percent_rank().over(w7))
)

enriched_products_2 = (
    scored
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.price",
        "a.stock_qty",
        "a.supplier_id",
        "a.stock_qty_scaled",
        "a.running_max_cost",
        "a.running_avg_cost",
        "a.stock_qty_scaled_avg_3_prior",
        "a.supplier_id_stddev_samp_stock_qty",
        "a.running_running_sum_cost",
        "a.seq_no",
        "a.running_avg_cost_percent_rank",
        F.col("d.supplier_name").alias("supplier_supplier_name"),
        F.col("d.rating"),
    )
)

result = (
    enriched_products_2
    .select(
        "id",
        "supplier_id",
        "running_avg_cost_percent_rank",
        "running_avg_cost",
        F.col("rating").alias("RATING_VAL"),
    )
    .unpivot(
        ids=["id", "supplier_id"],
        values=["running_avg_cost_percent_rank", "running_avg_cost", "RATING_VAL"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
