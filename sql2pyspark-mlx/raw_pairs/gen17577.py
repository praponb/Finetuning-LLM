from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") <= 190), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
        F.when(F.col("discount") >= 0.36, "HIGH").otherwise("LOW").alias("discount_tier"),
        F.when(F.col("amount") >= 1400, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("completed_amount"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
    )
    .orderBy(F.col("activity_month").desc())
)
