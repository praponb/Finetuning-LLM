from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/home") & (F.col("page") == "/checkout") & ~F.col("page").isin(["/cart", "/product", "/search"]))
    .select(
        "id",
        "duration_sec",
        F.when(F.col("event_type").isin(["click", "search"]), "Priority").otherwise("Other").alias("event_type_group"),
    )
    .orderBy(F.col("duration_sec").desc(), F.col("id").desc())
)
