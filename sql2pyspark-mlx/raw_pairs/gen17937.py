from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("revenue", "id")
w_running = Window.partitionBy("product_id").orderBy(F.col("revenue_int").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy("revenue_int_percent_rank", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("seq_no", "id")
w6 = Window.partitionBy("product_id").orderBy("last_known_revenue_int", "id")
w7 = Window.partitionBy("product_id")
w8 = Window.orderBy("units_avg_3", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("product_id").orderBy(F.col("seq_no_bucket").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("revenue") <= 1500)
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "product_id",
        "units",
        "revenue_int",
        F.last("revenue_int", ignorenulls=True).over(w_grp).alias("last_known_revenue_int"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.percent_rank().over(w_running).alias("revenue_int_percent_rank"),
    )
)

scored = (
    with_windows
    .filter(F.col("seq_no") < 3)
    .select(
        "*",
        F.avg("units").over(w4).alias("units_avg_3"),
        F.sum("units").over(w_grp).alias("running_units"),
    )
)

with_windows_store_sales = (
    scored
    .select(
        "*",
        F.ntile(4).over(w5).alias("seq_no_bucket"),
        F.percent_rank().over(w_running).alias("revenue_int_percent_rank_2"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

metrics = (
    with_windows_store_sales
    .select(
        "id",
        "units",
        "revenue_int",
        "last_known_revenue_int",
        "seq_no",
        "running_units",
        F.max("running_units").over(w7).alias("product_id_max_running_units"),
        F.avg("units_avg_3").over(w8).alias("units_avg_3_avg_3_prior"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("revenue_int") >= 70, "HIGH").otherwise("LOW").alias("revenue_int_tier"),
        F.when(F.col("units_avg_3_avg_3_prior") >= 33, "PREMIUM").otherwise("STANDARD").alias("units_avg_3_avg_3_prior_tier"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
)
