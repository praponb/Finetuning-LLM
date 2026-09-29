from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.year("launch_date"), "store_id")
w4 = Window.partitionBy("product_id", "store_id").orderBy("launch_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy(F.month("launch_date"), "store_id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-02-20").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.least("revenue", "units").alias("least_revenue_units"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.launch_date"), F.col("b.price"))
)

joined = (
    enriched
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.stock_qty"), F.col("b.product_name"))
)

metrics = (
    joined
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w_recent))
    .withColumn("next_units", F.lead("units").over(w_recent))
    .withColumn("prev_units", F.lag("units").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("price_avg_6", F.avg("price").over(w2))
)

with_windows = (
    metrics
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "launch_date",
        "price",
        "stock_qty",
        "next_stock_qty",
        "prev_units",
        "row_seq",
        "price_avg_6",
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
        F.avg("next_units").over(w4).alias("next_units_avg_5_prior"),
        F.corr("revenue", "units").over(w5).alias("corr_revenue_units"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("stock_qty") >= 409, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
    )
    .filter(F.col("avg_row_seq") > 0)
)
