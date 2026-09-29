from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy(F.col("rn").desc(), "id")
w6 = Window.orderBy(F.col("units_bucket").desc(), "id")
w7 = Window.partitionBy("product_id", "store_id").orderBy(F.col("row_seq").desc_nulls_last())
w8 = Window.partitionBy("product_id").orderBy(F.col("units_bucket").desc_nulls_last())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-23").cast("date"))
    .select("*", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
)

metrics = (
    cleaned
    .withColumn("units_bucket", F.ntile(4).over(w))
    .withColumn("next_units", F.lead("units", 2).over(w2))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .withColumn("product_id_stddev_samp_units_bucket", F.stddev_samp("units_bucket").over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

joined = (
    enriched
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"))
)

with_windows = (
    joined
    .select(
        "*",
        F.rank().over(w7).alias("row_seq_rank"),
        F.percent_rank().over(w8).alias("units_bucket_percent_rank"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.launch_date"), F.col("d.price"))
)

result = (
    with_dims
    .select(
        "id",
        "product_id",
        F.col("revenue").alias("REVENUE"),
        F.col("units_bucket_percent_rank").alias("UNITS_BUCKET_PERCENT_RANK"),
        F.col("revenue_per_unit").alias("REVENUE_PER_UNIT_VAL"),
        F.col("price").alias("PRICE_VAL"),
    )
    .unpivot(
        ["id", "product_id"],
        ["REVENUE", "UNITS_BUCKET_PERCENT_RANK", "REVENUE_PER_UNIT_VAL", "PRICE_VAL"],
        "kpi",
        "reading",
    )
)
