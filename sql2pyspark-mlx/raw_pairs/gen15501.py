from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "category", "cost", "price")
    .unpivot(
        ids=["id", "category"],
        values=["cost", "price"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
