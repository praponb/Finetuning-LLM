from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id", "amount", "discount", "shipping_fee")
    .unpivot(
        ids=["id", "customer_id"],
        values=["amount", "discount", "shipping_fee"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
