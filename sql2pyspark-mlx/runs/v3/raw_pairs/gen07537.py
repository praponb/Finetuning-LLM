from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "channel",
        "status",
        "discount",
        F.when(F.col("amount") >= 1360, "High").when(F.col("amount") >= 740, "Medium").otherwise("Low").alias("amount_level"),
    )
    .orderBy("status")
)
