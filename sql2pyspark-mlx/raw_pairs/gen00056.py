from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", "created_at", F.inline("items"))

result = (
    lines
    .groupBy("product_id", "customer_id")
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
    )
)
