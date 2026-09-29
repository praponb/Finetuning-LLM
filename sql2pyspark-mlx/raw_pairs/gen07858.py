from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select(
        "id",
        "category",
        F.col("cost").alias("COST_VAL"),
        F.col("price").alias("PRICE"),
    )
    .unpivot(
        ids=["id", "category"],
        values=["COST_VAL", "PRICE"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .cube(
        F.when(F.col("metric_value") >= 70, "HIGH").otherwise("LOW").alias("metric_value_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("metric_name")).alias("metric_name_list"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
    .filter(F.col("fashion_count") < 1000)
)
