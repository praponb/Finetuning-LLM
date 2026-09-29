from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("brand").isNull() & (F.col("category") == "Electronics"))

category_summary = (
    valid_products
    .groupBy("category")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    category_summary
    .orderBy("brand_values", "category")
    .limit(1)
)
