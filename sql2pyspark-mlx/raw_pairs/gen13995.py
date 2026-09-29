from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.array_contains("tags", "new") & F.col("brand").isin(["Globex", "Acme"])))
    .groupBy(
        F.quarter("launch_date").alias("launch_quarter"),
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        "category",
    )
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)
