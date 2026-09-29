from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & F.month("delivered_date").isin([2, 7, 12]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.round((F.col("cost") * 1.07), 2).alias("cost_scaled"),
    )
    .orderBy("cost_scaled")
)
