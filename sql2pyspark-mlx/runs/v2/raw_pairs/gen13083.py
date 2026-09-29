from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.countDistinct("units").alias("unique_stores"),
        F.min("revenue").alias("min_revenue"),
    )
)
