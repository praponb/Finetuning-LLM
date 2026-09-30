from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("brand")
w3 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("price", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("supplier_id").orderBy("corr_stock_qty_cost", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w7 = Window.orderBy(F.col("first_known_price").desc(), "id")

valid = (
    df_products
    .filter(F.col("brand") == "Stark")
    .select("*", F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"))
)

scored = (
    valid
    .withColumn("stock_qty_filled_avg_3_prior", F.avg("stock_qty_filled").over(w))
    .withColumn("brand_min_price", F.min("price").over(w_ordered))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w3))
    .withColumn("corr_stock_qty_cost", F.corr("stock_qty", "cost").over(w_ordered))
    .withColumn("price_avg_5_prior", F.avg("price").over(w4))
)

metrics = (
    scored
    .select(
        "*",
        F.avg("price").over(w5).alias("price_avg_6_prior"),
        F.ntile(5).over(w6).alias("stock_qty_bucket"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

stats = (
    metrics
    .groupBy(
        F.when(F.col("event_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        F.date_trunc("month", "launch_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
)

result = (
    stats
    .select("event_seq_tier", "activity_month", "acme_rows", "price_quartiles")
    .orderBy(F.col("acme_rows").desc_nulls_first())
)
