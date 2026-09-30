from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").isNotNull())
    .select("sale_date", F.lit("store_sale").alias("record_type"))
    .union(
        df_inventory
        .filter(F.col("last_restock").isNotNull())
        .select("last_restock", F.lit("inventory").alias("record_type"))
    )
    .distinct()
)
