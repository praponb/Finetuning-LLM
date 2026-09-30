from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2023-03-31") & (F.col("supplier_id") == 38))
    .groupBy(
        "category",
        F.when(F.col("cost") >= 550, "high").when(F.col("cost") >= 120, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("stock_qty")).otherwise(0)).alias("acme_stock_qty"),
    )
    .orderBy(F.col("acme_stock_qty").desc(), "unique_categorys")
)
