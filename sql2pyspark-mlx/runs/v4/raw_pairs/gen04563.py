from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.size("tags") >= 1) & (F.year("launch_date") == 2023) & (F.col("category") == "Grocery"))
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
)

brand_summary = (
    base
    .groupBy("brand", "category")
    .agg(
        F.max("price").alias("max_price"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
)

result = (
    brand_summary
    .filter(F.col("max_price") >= 1110)
    .orderBy("category", "brand")
    .limit(1)
)
