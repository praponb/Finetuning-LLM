from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-07-07"))) & F.month("last_restock").isin([9, 10, 12]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

result = (
    base
    .withColumn("activity_month", F.date_trunc("month", "last_restock"))
    .groupingSets(
        [["product_id", "activity_month"], ["product_id"], []],
        "product_id",
        "activity_month",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("days_since_last_restock"), 2).alias("avg_days_since_last_restock"),
        F.avg("days_since_last_restock").alias("avg_days_since_last_restock_2"),
        F.max("quantity").alias("max_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.grouping_id().alias("gid"),
    )
)
