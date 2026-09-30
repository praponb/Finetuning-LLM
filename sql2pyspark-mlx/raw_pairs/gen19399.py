from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "status", "discount", "shipping_fee")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "shipping_fee"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
