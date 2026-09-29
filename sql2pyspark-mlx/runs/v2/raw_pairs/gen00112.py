from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() & F.col("page").isNull() & F.col("page").isin(["/search", "/cart"]))
    .groupBy("session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
    .orderBy("page_values")
)
