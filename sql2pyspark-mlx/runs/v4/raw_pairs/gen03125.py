from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
    )
    .filter(F.col("globex_events") > 1)
    .orderBy(F.col("supplier_id").desc_nulls_last())
)
