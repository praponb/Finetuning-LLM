from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
    .filter(F.col("pct_stark") > 42)
    .select("brand", "pct_stark", "brand_values")
)
