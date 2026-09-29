from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 360)
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "order_id",
        F.col("unit_price").alias("UNIT_PRICE_VAL"),
        F.col("line_total").alias("LINE_TOTAL"),
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["UNIT_PRICE_VAL", "LINE_TOTAL"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
