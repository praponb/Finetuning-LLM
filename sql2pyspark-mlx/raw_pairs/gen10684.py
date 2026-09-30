from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc_nulls_first(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-04").cast("date"))
    .select(
        "*",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

enriched = (
    base
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "revenue_per_unit",
        "days_since_sale_date",
        F.sum("days_since_sale_date").over(w_running).alias("running_days_since_sale_date"),
        F.avg("units").over(w2).alias("units_avg_2"),
        F.ntile(10).over(w3).alias("revenue_bucket"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "sale_date", "revenue", "days_since_sale_date", "revenue_bucket")
)

cleaned = (
    latest
    .filter((F.col("sale_date") >= F.lit("2023-08-27").cast("date")) & (F.col("sale_date") > "2023-08-01"))
    .select(
        "id",
        "days_since_sale_date",
        "revenue_bucket",
        (F.datediff(F.current_date(), "sale_date") / 365.25).alias("years_since_sale_date"),
        F.least("revenue_bucket", "revenue").alias("least_revenue_bucket_revenue"),
    )
)

result = (
    cleaned
    .select("id", "revenue_bucket", "least_revenue_bucket_revenue")
    .orderBy(F.col("revenue_bucket").desc_nulls_first())
)
