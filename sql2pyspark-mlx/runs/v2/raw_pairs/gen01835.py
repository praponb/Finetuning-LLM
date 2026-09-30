from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") < 1520) & ~F.col("method").isin(["paypal", "card"])) | F.col("method").isNotNull())
    .withColumn("method_length", F.length("method"))
    .withColumn("method_status", F.concat("method", F.lit("/"), "status"))
)
