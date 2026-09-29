from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id")
    .intersect(
        df_inventory
        .filter(F.col("product_id") == 9)
        .select("product_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .groupingSets(
        [["product_id", "store_id", "activity_month"], ["product_id", "store_id"], []],
        "product_id",
        "store_id",
        "activity_month",
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.grouping("product_id").alias("gid"),
    )
    .orderBy(F.col("activity_month").asc_nulls_last())
)
