from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("stock_qty_per_acme"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
    )
    .orderBy(F.col("stock_qty_per_acme").asc_nulls_first())
)
