from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "category", "cost", "price")
    .unpivot(
        ids=["id", "category"],
        values=["cost", "price"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)

result = (
    unpivoted
    .cube(
        "metric",
        F.when(F.col("value") >= 63, "HIGH").otherwise("LOW").alias("value_tier"),
    )
    .agg(
        F.count_if(F.col("metric") == "cost").alias("cost_events"),
        F.max(F.when(F.col("category") == "Grocery", F.col("value"))).alias("grocery_max_value"),
        F.sum(F.when(F.col("category") == "Sports", F.col("value")).otherwise(0)).alias("sports_value"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("sports_value") > 0)
    .orderBy(F.col("value_tier").desc())
)
