from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "status", "amount", "discount")
    .unpivot(
        ids=["id", "status"],
        values=["amount", "discount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
