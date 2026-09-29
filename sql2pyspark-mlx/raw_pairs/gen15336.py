from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("revenue").desc(), "id")
w_running = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("units") < 17)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("units") >= 43, "High").when(F.col("units") >= 11, "Medium").otherwise("Low").alias("units_tier"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.units",
        "src.revenue",
        "src.units_tier",
        F.col("d.city"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered_store_sales = (
    deduped
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.round((F.col("row_num") * 1.15), 2).alias("row_num_scaled"),
        (F.col("row_num") * F.col("units")).alias("row_num_times_units"),
    )
)

enriched_store_sales = (
    filtered_store_sales
    .select(
        "*",
        F.sum("row_num_scaled").over(w_running).alias("running_sum_row_num_scaled"),
        F.avg("row_num_scaled").over(w_running).alias("running_avg_row_num_scaled"),
        F.max("row_num_scaled").over(w_running).alias("running_max_row_num_scaled"),
    )
)

summary = (
    enriched_store_sales
    .groupBy(
        F.when(F.col("running_sum_row_num_scaled") >= 310, "HIGH").otherwise("LOW").alias("running_sum_row_num_scaled_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.avg(F.when(F.col("city") == "Bangkok", F.col("running_sum_row_num_scaled"))).alias("bangkok_avg_running_sum_row_num_scaled"),
    )
)

result = (
    summary
    .select(
        "running_sum_row_num_scaled_tier",
        "row_count",
        "bangkok_avg_running_sum_row_num_scaled",
    )
    .orderBy(F.col("running_sum_row_num_scaled_tier").asc_nulls_last())
)
