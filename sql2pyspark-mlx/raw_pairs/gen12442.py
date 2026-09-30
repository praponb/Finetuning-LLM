from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.stddev_samp("price").alias("price_stddev"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("cost_per_grocery"),
    )
)
