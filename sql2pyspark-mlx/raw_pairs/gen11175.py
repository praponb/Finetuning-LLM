from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy("product_id", "store_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").asc_nulls_first(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-03")))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

scored = (
    cleaned
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        F.sum("units").over(w).alias("running_units"),
        F.ntile(10).over(w2).alias("revenue_per_unit_bucket"),
        F.lead("revenue_per_unit", 1).over(w3).alias("next_revenue_per_unit"),
        F.sum("revenue_per_unit").over(w4).alias("product_id_sum_revenue_per_unit"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    deduped
    .filter(F.col("units") <= 19)
    .select(
        "*",
        F.greatest("revenue", "product_id_sum_revenue_per_unit").alias("greatest_revenue_product_id_sum_revenue_per_unit"),
    )
)

result = (
    valid
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("max_revenue") < 1000000)
    .orderBy("event_count")
)
