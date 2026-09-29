from pyspark.sql import functions as F

src = (
    df_products
    .filter(~F.col("brand").isin(["Umbrella", "Acme"]))
    .withColumn("product_name_length", F.length("product_name"))
)

products_stats = (
    src
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        F.when(F.col("stock_qty") >= 407, "high").when(F.col("stock_qty") >= 70, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
    )
)

result = (
    products_stats
    .filter(F.col("pct_umbrella") > 34)
    .select("launch_month", "stock_qty_bucket", "initech_count", "pct_umbrella")
    .orderBy("stock_qty_bucket", F.col("initech_count").desc())
)
