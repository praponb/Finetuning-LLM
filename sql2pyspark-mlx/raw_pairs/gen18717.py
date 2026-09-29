from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", "cost", "price")
    .unpivot(
        ids=["id", "brand"],
        values=["cost", "price"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
