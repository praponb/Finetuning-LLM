from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy(F.col("revenue").desc(), "id")

enriched = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.launch_date"), F.col("p.cost"), F.col("p.stock_qty"))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .select(
        "*",
        F.avg("units").over(w2).alias("units_avg_3_prior"),
        F.sum("cost").over(w3).alias("running_cost"),
        F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4).alias("revenue_share"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("running_cost", 0.9).alias("running_cost_p90"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.max("cost").alias("max_cost"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.min("revenue_share").alias("min_revenue_share"),
    )
    .orderBy(F.col("min_revenue_share").desc())
)
