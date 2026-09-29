from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.month("launch_date").alias("launch_month"), "supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
        F.min("launch_date").alias("first_launch_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
    .filter(F.col("pct_globex") >= 52)
)
