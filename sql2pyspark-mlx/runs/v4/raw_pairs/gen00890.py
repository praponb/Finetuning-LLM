from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("activity_month", F.date_trunc("month", "order_date"))
    .groupingSets(
        [["activity_month", "store_id"], ["activity_month"]],
        "activity_month",
        "store_id",
    )
    .agg(
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_per_mobile"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("amount_per_completed"),
    )
)
