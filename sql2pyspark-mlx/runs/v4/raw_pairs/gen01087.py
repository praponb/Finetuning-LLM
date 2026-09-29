from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 2800))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.avg("units").alias("avg_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
)
