from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 166)
    .select(
        "id",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
