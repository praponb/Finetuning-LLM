from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "category", "price", "cost")
    .unpivot(
        ids=["id", "category"],
        values=["price", "cost"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("value") >= 89, "LARGE").otherwise("SMALL").alias("value_tier"),
    )
    .agg(
        F.sum(F.when(F.col("kpi") == "price", F.col("value")).otherwise(0)).alias("price_value"),
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
    )
    .filter(F.col("price_value") >= 0)
    .orderBy(F.col("price_value").desc())
)
