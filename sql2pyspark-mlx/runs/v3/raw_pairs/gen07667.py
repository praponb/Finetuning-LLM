from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("shipping_fee") < 37) & F.col("channel").isin(["web", "mobile"])) | ~F.col("channel").isin(["web", "store", "partner"]))
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 4)
    .orderBy(F.col("cnt").desc())
)
