from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel", "shipping_fee", "amount", "discount")
    .unpivot(
        ids=["id", "channel"],
        values=["shipping_fee", "amount", "discount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
