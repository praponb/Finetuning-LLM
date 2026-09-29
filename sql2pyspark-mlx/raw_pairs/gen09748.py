from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .subtract(
        df_inventory
        .filter(F.month("last_restock").isin([4, 7, 8]))
        .select("last_restock")
    )
    .orderBy(F.col("end_date").desc())
)
