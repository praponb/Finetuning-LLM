from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["cost", "price"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("metric")).alias("metric_list"),
        F.count(F.when(F.col("metric") == "price", 1)).alias("price_count"),
        F.count_if(F.col("metric") == "cost").alias("cost_events"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("metric") == "cost", F.col("metric_value")).otherwise(0)).alias("cost_metric_value"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("num_rows")
)
