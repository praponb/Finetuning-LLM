from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.col("hire_date").alias("event_date"))
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([3, 4, 5, 6]))
        .select(F.col("last_restock").alias("event_date"))
    )
    .orderBy("event_date")
)
