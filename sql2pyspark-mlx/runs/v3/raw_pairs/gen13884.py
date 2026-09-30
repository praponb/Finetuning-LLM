from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([26, 32]))
    .select(
        "order_id",
        "cost",
        "carrier",
        F.round((F.col("cost") * 0.85), 2).alias("cost_scaled"),
    )
    .orderBy("order_id")
)
