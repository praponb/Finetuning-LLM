from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("cumulative_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("running_min_sale_date_quarter", "id")
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("product_id_avg_cumulative_cumulative_units_2_tier").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.quarter("sale_date").alias("sale_date_quarter"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "product_id",
        "sale_date_quarter",
        F.sum("units").over(w).alias("cumulative_units"),
        F.sum("units").over(w2).alias("cumulative_units_2"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.product_name"))
)

scored = (
    joined
    .withColumn("running_sum_sale_date_quarter", F.sum("sale_date_quarter").over(w_grp))
    .withColumn("running_avg_sale_date_quarter", F.avg("sale_date_quarter").over(w_grp))
    .withColumn("running_min_sale_date_quarter", F.min("sale_date_quarter").over(w_grp))
    .withColumn("running_count_sale_date_quarter", F.count("sale_date_quarter").over(w_grp))
    .withColumn("cumulative_cumulative_units_2", F.sum("cumulative_units_2").over(w))
)

with_windows = (
    scored
    .filter(F.col("cumulative_units") < 157)
    .select(
        "id",
        "product_id",
        "sale_date_quarter",
        "cumulative_units",
        "cumulative_cumulative_units_2",
        F.lag("running_count_sale_date_quarter").over(w4).alias("prev_running_count_sale_date_quarter"),
        F.lag("running_min_sale_date_quarter").over(w4).alias("prev_running_min_sale_date_quarter"),
        F.lead("running_min_sale_date_quarter").over(w4).alias("next_running_min_sale_date_quarter"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("cumulative_cumulative_units_2").over(w5).alias("product_id_avg_cumulative_cumulative_units_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("product_id_avg_cumulative_cumulative_units_2") >= 1020, "HIGH").otherwise("LOW").alias("product_id_avg_cumulative_cumulative_units_2_tier"),
        F.when(F.col("cumulative_cumulative_units_2") >= 921, "LARGE").otherwise("SMALL").alias("cumulative_cumulative_units_2_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_running_count_sale_date_quarter", [0.25, 0.5, 0.75]).alias("prev_running_count_sale_date_quarter_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w6))
    .orderBy(
        F.col("product_id_avg_cumulative_cumulative_units_2_tier").asc_nulls_first(),
    )
)
