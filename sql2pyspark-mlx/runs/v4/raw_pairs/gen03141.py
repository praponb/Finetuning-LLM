from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w_part = Window.orderBy("units", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("seq_no").asc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy("seq_no", "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-09").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
    )
)

scored = (
    base
    .withColumn("units_percent_rank", F.percent_rank().over(w))
    .withColumn("seq_no", F.row_number().over(w_part))
)

scored_store_sales = (
    scored
    .select(
        "*",
        F.ntile(3).over(w_part).alias("units_bucket"),
        F.ntile(10).over(w3).alias("units_bucket_2"),
        F.lag("units_percent_rank", 2, 0).over(w_part).alias("prev_units_percent_rank"),
        F.rank().over(w4).alias("seq_no_rank"),
        F.ntile(5).over(w5).alias("seq_no_bucket"),
    )
)

enriched = (
    scored_store_sales
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.stock_qty"), F.col("d.brand"))
)

result = (
    enriched
    .groupBy(
        "brand",
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        F.when(F.col("units_bucket_2") >= 7, "LARGE").otherwise("SMALL").alias("units_bucket_2_tier"),
    )
    .agg(
        F.percentile_approx("units_percent_rank", 0.95).alias("units_percent_rank_p95"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
    )
    .filter(F.col("globex_events") >= 1)
    .orderBy(F.col("globex_events").desc())
)
