from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(530, 1440) & ~F.col("status").isin(["FAILED", "SUCCESS"]))
    .select(
        "id",
        "order_id",
        F.when(F.col("amount") >= 1670, "High").when(F.col("amount") >= 660, "Medium").otherwise("Low").alias("amount_band"),
    )
)
