from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .groupBy(
        F.when(F.col("page").isin(["/home", "/search"]), "tier_1").otherwise("other").alias("page_group"),
        "page",
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy("row_count")
)
