from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").isNotNull())
    .select("last_restock", F.lit("inventory").alias("record_type"))
    .union(
        df_stores
        .filter(F.col("opened_date").isNotNull())
        .select("opened_date", F.lit("store").alias("record_type"))
    )
    .distinct()
)
