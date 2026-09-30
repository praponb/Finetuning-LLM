from pyspark.sql import functions as F

by_dimension = (
    df_orders
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .groupingSets(
        [["region", "status"], ["region"], []],
        F.col("d.region").alias("region"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.shipping_fee")).alias("max_shipping_fee"),
        F.round(F.avg(F.col("src.amount")), 2).alias("avg_amount"),
    )
)

result = (
    by_dimension
    .select(
        "region",
        "cnt",
        "avg_amount",
        F.when(F.col("region").rlike("^(West|North)$"), "GROUP_A").otherwise("EXTENDED").alias("region_zone"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_first())
)
