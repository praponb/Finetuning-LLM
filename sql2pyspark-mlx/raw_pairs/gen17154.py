from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "supplier_id", F.col("cost").alias("COST"), "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["COST", "price"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
