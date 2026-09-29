from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
    .withColumn("line_total_2", (F.col("unit_price") + F.col("quantity")))
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        F.col("line_total").alias("LINE_TOTAL"),
        "unit_price",
        F.col("line_total_2").alias("LINE_TOTAL_2"),
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["LINE_TOTAL", "unit_price", "LINE_TOTAL_2"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
