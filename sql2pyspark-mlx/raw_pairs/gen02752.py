from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-03").cast("date"))
    .select(
        "*",
        F.round("stock_qty", 1).alias("stock_qty_rounded"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

result = (
    filtered
    .groupBy("category", F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.round(F.avg("stock_qty_rounded"), 2).alias("avg_stock_qty_rounded"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
    )
    .filter(F.col("avg_stock_qty_rounded") >= 0)
    .orderBy("avg_stock_qty_rounded")
)
