from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([7, 16, 18]))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.month("launch_date").alias("launch_month"),
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.avg("price").alias("avg_price"),
    )
    .orderBy(F.col("launch_month").desc(), "launch_cohort")
    .limit(20)
)
