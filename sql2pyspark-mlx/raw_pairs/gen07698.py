from pyspark.sql import functions as F

long_format = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    long_format
    .groupBy(
        F.when(F.col("metric_value") >= 65, "LARGE").otherwise("SMALL").alias("metric_value_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("measure")).alias("measure_list"),
        F.count(F.when(F.col("measure") == "cost", 1)).alias("cost_count"),
        F.round(F.avg("metric_value"), 2).alias("avg_metric_value"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
    )
)
