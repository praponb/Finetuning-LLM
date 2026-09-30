from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("discount") < 0.27)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["segment", "channel"], ["segment"]],
        F.col("p.segment").alias("segment"),
        F.col("s.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
        F.max(F.col("s.amount_filled")).alias("max_amount_filled"),
    )
    .orderBy(F.col("segment").desc_nulls_first())
)
