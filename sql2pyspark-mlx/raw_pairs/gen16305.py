from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 35) & (F.col("cost").between(79, 96) | (F.col("cost") > 56)))
    .select(
        "shipped_date",
        "delivered_date",
        F.when(F.col("cost") >= 92, "High").when(F.col("cost") >= 41, "Medium").otherwise("Low").alias("cost_tier"),
    )
    .orderBy("delivered_date")
)
