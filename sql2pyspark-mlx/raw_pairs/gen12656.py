from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([20, 23, 37, 39]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

result = (
    base
    .select("id", "order_id", "unit_price", "quantity_str")
    .unpivot(
        ids=["id", "order_id"],
        values=["unit_price", "quantity_str"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
