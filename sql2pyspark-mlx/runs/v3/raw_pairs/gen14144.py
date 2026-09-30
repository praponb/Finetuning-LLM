from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        "brand",
        F.when(F.col("brand").isin(["Initech", "Acme"]), "priority").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.sum(F.when(F.col("category") == "Home", F.col("price")).otherwise(0)).alias("home_price"),
    )
    .orderBy("brand_values", F.col("brand").desc())
)
