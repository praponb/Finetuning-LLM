from pyspark.sql import functions as F

result = (
    df_orders
    .rollup(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.when(F.col("amount") >= 1220, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("web_amount").desc_nulls_last())
)
