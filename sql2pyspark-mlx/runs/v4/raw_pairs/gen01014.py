from pyspark.sql import functions as F

src = df_web_events.filter(~F.col("page").isin(["/home", "/cart", "/product"]))

event_type_summary = (
    src
    .groupBy("event_type", F.to_date("event_time").alias("event_day"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
)

result = (
    event_type_summary
    .select("event_type", "event_day", "event_type_values", "pct_product")
)
