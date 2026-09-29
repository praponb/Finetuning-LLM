from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Umbrella", "Acme"]))
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .filter(F.col("avg_price") >= 39)
    .select("supplier_id", "avg_price", "category_values")
    .orderBy(F.col("category_values").desc(), F.col("avg_price").desc())
)
