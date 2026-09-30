from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Kerry"]))
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .pivot("status", ["IN_TRANSIT", "DELIVERED", "RETURNED", "LOST"])
    .agg(F.count("cost"))
)
