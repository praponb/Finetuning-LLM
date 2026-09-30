from pyspark.sql import functions as F

with_dims = (
    df_orders
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.sqft"), F.col("p.region"), F.col("p.store_name"))
)

result = (
    with_dims
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "sqft",
        "store_name",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
