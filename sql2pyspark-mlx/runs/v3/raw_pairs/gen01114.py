from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 15, 20]))
    .select(F.col("hire_date").alias("event_date"))
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 4, 5]))
        .select(F.col("last_restock").alias("event_date"))
    )
    .distinct()
)
