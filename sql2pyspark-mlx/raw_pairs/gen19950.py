from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "supplier_id", F.col("price").alias("PRICE_VAL"), "cost")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["PRICE_VAL", "cost"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
