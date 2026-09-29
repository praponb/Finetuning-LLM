from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 2)
    .groupBy(
        "category",
        F.when(F.col("brand").isin(["Initech", "Stark"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("cost_range") > 22)
    .orderBy(F.col("cost_range").desc())
)
