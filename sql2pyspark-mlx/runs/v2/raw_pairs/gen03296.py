from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "partner", "mobile"]))
    .orderBy(F.col("shipping_fee").desc())
)
