from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").isNotNull())
    .select("shipped_date")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").isNotNull())
        .select("last_restock")
    )
    .orderBy(F.col("shipped_date").desc())
)
