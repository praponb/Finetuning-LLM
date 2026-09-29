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
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("measure") == "cost", F.col("value")).otherwise(0)).alias("cost_value"),
        F.count(F.when(F.col("measure") == "price", 1)).alias("price_rows"),
        F.size(F.collect_set("measure")).alias("num_measures"),
        F.avg(F.when(F.col("measure") == "price", F.col("value"))).alias("price_avg_value"),
        F.round(F.avg("value"), 2).alias("avg_value"),
    )
    .filter(F.col("price_rows") < 1000)
)
