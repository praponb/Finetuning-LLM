from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.year("sale_date") >= 2023) & F.col("store_id").isin([2, 3, 6]))
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy(F.col("units_range").desc(), "product_id", "store_id")
    .limit(5)
)
