from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") != "RETURNED") & (F.year("delivered_date") >= 2025))
    .groupBy("carrier")
    .agg(F.countDistinct("cost").alias("unique_orders"))
    .filter(F.col("unique_orders") > 1)
    .orderBy(F.col("unique_orders").desc(), "carrier")
    .limit(10)
)
