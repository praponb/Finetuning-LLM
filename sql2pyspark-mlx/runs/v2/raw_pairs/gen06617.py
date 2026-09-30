from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([5, 8, 9]))
    .groupBy("status")
    .agg(F.countDistinct("discount").alias("unique_statuss"))
    .filter(F.col("unique_statuss") < 9)
    .select("status", "unique_statuss")
    .orderBy("status")
    .limit(10)
)
