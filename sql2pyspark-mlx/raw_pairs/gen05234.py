from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "sale_date"), "product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("region").orderBy("revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("revenue").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("revenue_bucket", "id")
w8 = Window.partitionBy(F.year("launch_date"), "city").orderBy(F.col("revenue_bucket").desc(), "id")
w9 = Window.partitionBy("region", "city").orderBy("prev_cumulative_revenue", "id")
w10 = Window.partitionBy("store_id")

scored = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 8]))
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(4).over(w2).alias("revenue_bucket"),
    )
)

cleaned = (
    scored
    .filter(F.col("sale_date") > F.lit("2023-10-05").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "event_seq",
        "revenue_bucket",
        F.round((F.col("revenue") / F.col("event_seq")), 2).alias("revenue_per_event_seq"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.revenue",
        "a.revenue_bucket",
        F.col("b.city"),
        F.col("b.store_name"),
        F.col("b.region"),
    )
)

scored_store_sales = (
    joined
    .withColumn("revenue_bucket_share", F.round((F.col("revenue_bucket") / F.sum("revenue_bucket").over(w3)), 4))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w4))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

joined_store_sales = (
    scored_store_sales
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.category"), F.col("b.launch_date"))
)

metrics = (
    joined_store_sales
    .withColumn("prev_revenue_bucket_share", F.lag("revenue_bucket_share").over(w_grp))
    .withColumn("next_revenue_bucket_share", F.lead("revenue_bucket_share").over(w_grp))
    .withColumn("next_cumulative_revenue", F.lead("cumulative_revenue").over(w_grp))
    .withColumn("prev_cumulative_revenue", F.lag("cumulative_revenue").over(w_grp))
    .withColumn("event_seq_2", F.row_number().over(w_grp))
    .withColumn("row_seq", F.row_number().over(w8))
)

result = (
    metrics
    .select(
        "*",
        F.lead("prev_cumulative_revenue").over(w9).alias("next_prev_cumulative_revenue"),
        F.lead("cumulative_revenue").over(w9).alias("next_cumulative_revenue_2"),
        F.row_number().over(w9).alias("event_seq_3"),
        F.round((F.col("event_seq_2") / F.sum("event_seq_2").over(w10)), 4).alias("event_seq_2_share"),
    )
)
