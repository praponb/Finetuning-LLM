from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") < "2024-10-12") & F.col("supplier_id").isin([11, 16, 32]) & (F.col("price") <= 640))
)

launch_cohort_summary = (
    base
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
)

result = (
    launch_cohort_summary
    .filter(F.col("stock_qty_range") >= 38)
    .orderBy(F.col("stock_qty_range").desc())
)
