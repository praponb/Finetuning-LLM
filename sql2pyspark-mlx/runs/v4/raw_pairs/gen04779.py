from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

enriched = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.country"), F.col("d.supplier_name"))
)

filtered = (
    enriched
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-04-21")))
    .withColumn("brand_prefix", F.substring("brand", 1, 4))
    .withColumn("tags_text", F.array_join("tags", "|"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

result = (
    latest_match
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("store_sale_revenue")).otherwise(0)).alias("globex_store_sale_revenue"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
    .orderBy(F.col("country").desc())
)
