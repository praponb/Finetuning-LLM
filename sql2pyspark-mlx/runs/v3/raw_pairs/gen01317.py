from pyspark.sql import functions as F

src = (
    df_products
    .filter(~F.col("category").isin(["Sports", "Grocery", "Fashion"]) & F.col("product_name").like("%Sho%") & F.col("supplier_id").isin([3, 8, 36]))
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
)

summary = (
    src
    .groupBy("brand", "category")
    .agg(
        F.avg("price").alias("avg_price"),
        F.countDistinct("price").alias("unique_categorys"),
    )
)

result = summary.filter(F.col("avg_price") > 530)
