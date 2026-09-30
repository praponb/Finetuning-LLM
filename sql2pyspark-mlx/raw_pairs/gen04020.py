from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "status", "discount", "shipping_fee", "amount")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "shipping_fee", "amount"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
