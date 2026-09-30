from pyspark.sql import functions as F

src = df_products.filter(F.col("brand") != "Globex")

launch_month_summary = (
    src
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("launch_month"),
        F.year("launch_date").alias("launch_year"),
    )
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
)

result = (
    launch_month_summary
    .select("launch_month", "launch_year", "price_range")
    .orderBy(F.col("launch_month").desc())
)
