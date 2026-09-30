from pyspark.sql import functions as F

src = (
    df_products
    .filter((F.col("brand") == "Umbrella") | (F.col("stock_qty") <= 368))
    .withColumn("tags_text", F.array_join("tags", "|"))
)

brand_summary = (
    src
    .groupBy(
        "brand",
        F.when(F.col("cost") >= 660, "high").when(F.col("cost") >= 220, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    brand_summary
    .select("brand", "cost_bucket", "stddev_cost")
    .orderBy("stddev_cost", F.col("brand").desc(), "cost_bucket")
    .limit(50)
)
