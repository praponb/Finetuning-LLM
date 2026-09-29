from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "category")
    .agg(
        F.countDistinct("stock_qty").alias("unique_categorys"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("unique_categorys") > 10)
    .select("launch_month", "category", "unique_categorys", "latest_launch_date")
)
