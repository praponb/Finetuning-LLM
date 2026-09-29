from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date")).orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date_day", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-26").cast("date"))
    .withColumn("sale_date_day", F.dayofmonth("sale_date"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.lead("sale_date_day", 1).over(w2).alias("next_sale_date_day"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .cube("product_id", "store_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.count("*").alias("event_count"),
    )
)
