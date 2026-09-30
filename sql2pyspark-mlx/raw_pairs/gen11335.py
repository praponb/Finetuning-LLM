from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(4, 7))
    .select(
        "*",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

result = (
    filtered
    .select(
        "id",
        "product_id",
        F.col("unit_price").alias("UNIT_PRICE"),
        F.col("line_total").alias("LINE_TOTAL_VAL"),
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["UNIT_PRICE", "LINE_TOTAL_VAL"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
