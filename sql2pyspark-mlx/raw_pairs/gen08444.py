from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .withColumn("sorted_tags", F.sort_array("tags"))
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

result = (
    base
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.launch_date")), "left_semi")
)
