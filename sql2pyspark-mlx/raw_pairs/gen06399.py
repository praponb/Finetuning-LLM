from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)

result = (
    metrics_long
    .groupBy(
        F.when(F.col("value") >= 63, "LARGE").otherwise("SMALL").alias("value_tier"),
    )
    .agg(
        F.sum(F.when(F.col("measure") == "cost", F.col("value")).otherwise(0)).alias("cost_value"),
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
    )
    .orderBy(F.col("value_tier").desc())
)
