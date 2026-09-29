from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "CANCELLED", "SHIPPED"]) & (F.col("channel") == "partner"))
    .select(
        "id",
        "status",
        "amount",
        "store_id",
        F.col("amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("id").desc(), "amount")
)
