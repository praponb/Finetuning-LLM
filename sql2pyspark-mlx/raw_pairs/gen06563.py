from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("running_sum_units").desc(), "id")
w4 = Window.orderBy(F.col("running_max_units").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("YEAR", "sale_date"), "product_id")
w6 = Window.partitionBy("product_id").orderBy("running_sum_units_share", "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-04-24"))) & (F.col("units") >= 17))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.date_add("sale_date", 64).alias("sale_date_plus_64d"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.max("units").over(w).alias("running_max_units"),
        F.min("units").over(w).alias("running_min_units"),
        F.sum("units").over(w).alias("running_sum_units"),
        F.count("units").over(w).alias("running_count_units"),
        F.ntile(10).over(w2).alias("units_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .withColumn("running_max_units_bucket", F.ntile(10).over(w4))
    .withColumn("running_sum_units_share", F.round((F.col("running_sum_units") / F.sum("running_sum_units").over(w5)), 4))
)

enriched_store_sales = (
    enriched
    .withColumn("next_running_sum_units", F.lead("running_sum_units").over(w6))
    .withColumn("next_units_bucket", F.lead("units_bucket").over(w6))
    .withColumn("prev_units_bucket", F.lag("units_bucket").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
)

result = (
    enriched_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "sale_date_plus_64d",
        "running_min_units",
        "running_count_units",
        "units_bucket",
        "row_num",
    )
    .orderBy(F.col("running_count_units").desc())
)
