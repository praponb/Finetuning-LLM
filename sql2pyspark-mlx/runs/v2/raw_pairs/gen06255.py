from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["SHIPPED", "RETURNED"]))
    .select("channel", "status")
    .distinct()
    .orderBy(F.col("status").desc())
)
