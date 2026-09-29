from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .groupBy("metric")
    .agg(
        F.percentile_approx("value", 0.95).alias("value_p95"),
        F.sum(F.when(F.col("metric") == "cost", F.col("value")).otherwise(0)).alias("cost_value"),
        F.round(F.avg("value"), 2).alias("avg_value"),
    )
    .orderBy(F.col("cost_value").desc_nulls_first())
)
