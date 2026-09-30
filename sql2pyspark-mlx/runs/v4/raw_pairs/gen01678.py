from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("price_per_initech"),
        F.stddev_samp("price").alias("price_stddev"),
        F.corr("price", "cost").alias("corr_price_cost"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
    )
    .filter(F.col("distinct_supplier_ids") > 1)
    .orderBy("cost_p50")
)
