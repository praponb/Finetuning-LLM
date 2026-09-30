from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("row_num", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-25").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.lag("revenue_per_unit").over(w2).alias("prev_revenue_per_unit"),
        F.lead("revenue_per_unit").over(w2).alias("next_revenue_per_unit"),
        F.lead("revenue").over(w2).alias("next_revenue"),
        F.lag("revenue").over(w2).alias("prev_revenue"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

filtered = (
    enriched
    .filter(F.col("sale_date") > F.lit("2023-09-16").cast("date"))
    .withColumn("greatest_revenue_row_num", F.greatest("revenue", "row_num"))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.launch_date"), F.col("p.brand"), F.col("p.stock_qty"))
)

result = (
    with_dims
    .groupBy("product_id")
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.bit_or("units").alias("units_bit_or"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("product_id").desc())
)
