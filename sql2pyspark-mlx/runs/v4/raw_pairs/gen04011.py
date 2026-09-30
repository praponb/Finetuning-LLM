from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        F.col("cost").alias("COST_VAL"),
        F.col("price").alias("PRICE"),
    )
    .unpivot(["id", "supplier_id"], ["COST_VAL", "PRICE"], "measure", "metric_value")
)
