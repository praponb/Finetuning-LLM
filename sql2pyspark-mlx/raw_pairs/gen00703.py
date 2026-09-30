from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
    )
    .filter(F.col("purchase_duration_sec") > 0)
    .orderBy("duration_sec_bit_and")
)
