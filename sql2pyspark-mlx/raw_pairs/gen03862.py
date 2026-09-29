from pyspark.sql import functions as F

long_format = (
    df_products
    .select("id", "category", "cost", "price")
    .unpivot(
        ids=["id", "category"],
        values=["cost", "price"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    long_format
    .groupBy(F.when(F.col("value") >= 89, "HIGH").otherwise("LOW").alias("value_tier"))
    .agg(
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.sum(F.when(F.col("metric") == "price", F.col("value")).otherwise(0)).alias("price_value"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("category").alias("distinct_categories"),
        F.sum(F.when(F.col("metric") == "cost", F.col("value")).otherwise(0)).alias("cost_value"),
    )
    .orderBy(F.col("value_tier").desc_nulls_last())
)
