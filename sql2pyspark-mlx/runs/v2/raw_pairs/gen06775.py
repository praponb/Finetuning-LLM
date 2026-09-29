from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1070)
    .withColumn("method_group", F.when(F.col("method").isin(["promptpay", "paypal"]), "Group A").otherwise("Other"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .orderBy("method_group", "id")
)
