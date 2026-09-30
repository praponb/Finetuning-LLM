from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 31) & ((F.col("amount") <= 630) | ~F.col("channel").isin(["partner", "mobile", "store"])))
    .select(
        "id",
        "amount",
        "customer_id",
        "status",
        F.when(F.col("amount") >= 1350, "High").when(F.col("amount") >= 450, "Medium").otherwise("Low").alias("amount_band"),
    )
    .orderBy(F.col("id").desc(), F.col("amount").desc())
)
