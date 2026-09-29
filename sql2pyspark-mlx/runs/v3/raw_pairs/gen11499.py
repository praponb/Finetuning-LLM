from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") != "Umbrella") | F.col("cost").isNull())
    .groupBy(
        "brand",
        F.when(F.col("brand").isin(["Globex", "Stark"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.min("launch_date").alias("first_launch_date"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy(F.col("first_launch_date").desc(), "brand")
)
