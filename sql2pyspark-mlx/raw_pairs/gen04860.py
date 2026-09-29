from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("order_id") == 22) & (F.col("order_id") == 32)) | F.col("order_id").isin([6, 21, 22, 31]))
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_delivered"),
    )
    .filter(F.col("pct_delivered") > 74)
)
