from pyspark.sql import functions as F

joined = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.rating").between(1.9, 2.2))
    .select("e.*", F.col("b.supplier_name"), F.col("b.country"), F.col("b.city"))
)

result = (
    joined
    .groupBy("supplier_id")
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("stock_qty_per_jp"),
        F.max("price").alias("max_price"),
        F.corr("cost", "stock_qty").alias("corr_cost_stock_qty"),
    )
    .filter(F.col("stock_qty_per_jp") > 0)
)
