from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/checkout"]))
    .select(
        "id",
        "page",
        F.concat("event_type", F.lit(" - "), "page").alias("event_type_page"),
    )
    .orderBy(F.col("event_type_page").desc(), "id")
    .limit(10)
)
