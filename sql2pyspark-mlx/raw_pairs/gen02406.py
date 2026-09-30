from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "store_id", "discount", "amount", "shipping_fee")
    .unpivot(
        ids=["id", "store_id"],
        values=["discount", "amount", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
